package com.example.loader.service.impl;

import com.example.loader.service.CopyLoadService;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * 高性能 COPY 装载实现：
 * 核心思想：
 *  1) 单线程顺序读取大文件（减少磁盘随机读）；
 *  2) 按「客户号 % 分片数」将每一行分发到对应的阻塞队列；
 *  3) 为每个分片启动一个写线程，持有各自的 CopyManager，
 *     使用 Pipe（PipedInputStream/OutputStream）把队列数据直接喂给 COPY FROM STDIN；
 *  4) 全程流式，无需中间临时文件，内存常量级；
 *  5) 坏数据写入 .bad 文件，方便后续修复回灌。
 */
@Slf4j
@Service
public class CopyLoadServiceImpl implements CopyLoadService {

    private final DataSource dataSource;

    public CopyLoadServiceImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 分表/并发数量，建议与实际分表数一致 */
    @Value("${loader.shards:16}")
    private int shards;

    /** 分表前缀，例如 tb_customer_equity_ => tb_customer_equity_01 ... */
    @Value("${loader.tablePrefix:tb_customer_equity_}")
    private String tablePrefix;

    /** 坏数据文件后缀 */
    @Value("${loader.badFileSuffix:.bad}")
    private String badSuffix;

    /** 计数落盘目录 */
    @Value("${loader.statDir:/tmp/8424227}")
    private String statDir;


    /** 时间格式校验器 */
    static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void loadFile() {
        // 从环境变量读取文件信息（题目要求）
        String filePath = System.getenv("FILE_PATH");
        String fileName = System.getenv("FILE_NAME_EQUITY");
        if (filePath == null || fileName == null) {
            throw new IllegalStateException("环境变量 FILE_PATH / FILE_NAME_EQUITY 未设置");
        }
        Path input = Paths.get(filePath, fileName);
        Path badPath = Paths.get(filePath, fileName + badSuffix);

        // 为每个分片准备一个阻塞队列，容量约 16K 行，可根据机器内存/吞吐调优
        ArrayBlockingQueue<byte[]>[] queues = new ArrayBlockingQueue[shards];
        for (int i = 0; i < shards; i++) queues[i] = new ArrayBlockingQueue<>(1 << 14);
        final boolean[] finished = {false}; // 读线程是否结束的标志

        // 启动写线程池：每个分片 1 线程，持有一个 CopyManager
        ExecutorService writers = Executors.newFixedThreadPool(shards);
        IntStream.range(0, shards).forEach(s -> writers.submit(() -> copyWorker(s, queues, finished)));

        long ok = 0, bad = 0; // 计数器：成功入队与坏行数量
        try (BufferedReader br = Files.newBufferedReader(input, StandardCharsets.UTF_8);
             BufferedWriter badOut = Files.newBufferedWriter(badPath, StandardCharsets.UTF_8,
                     StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            String line;
            while ((line = br.readLine()) != null) {
                // 以 | 分隔，保留空字段
                String[] arr = line.split("\\|", -1);
                if (arr.length != 5) { // 基础列数校验
                    badOut.write(line); badOut.newLine(); bad++; continue;
                }

                // === 字段清洗与轻校验 ===
                String tsRaw   = arr[0].trim();
                String custRaw = arr[1].trim();
                String accRaw  = arr[2].trim().replace(" ", "");
                String ccyRaw  = arr[3].trim();
                String balRaw  = arr[4].trim();

                if (!isTs(tsRaw)) { badOut.write(line); badOut.newLine(); bad++; continue; }

                // customerNo：数字、≤10；短的左补0；>10 fail
                if (!isAllDigits(custRaw) || custRaw.length() > 10) { badOut.write(line); badOut.newLine(); bad++; continue; }
                String cust = leftPadToLen(custRaw, 10);

                // accountNo：数字、≤18 合规；>18 fail；短的不补0
                if (!isAllDigits(accRaw) || accRaw.length() > 18) { badOut.write(line); badOut.newLine(); bad++; continue; }
                String acc = accRaw;

                // ccy：严格 3 位字母
                if (ccyRaw.length() != 3 || !isAlpha(ccyRaw)) { badOut.write(line); badOut.newLine(); bad++; continue; }
                String ccy = ccyRaw.toUpperCase();

                // balance：数值
                if (!balRaw.matches("^-?\\d{1,18}(\\.\\d{1,2})?$")) { badOut.write(line); badOut.newLine(); bad++; continue; }
                String bal = balRaw;

                // 重新按原分隔符拼接一行，末尾必须加换行（COPY 按行切分）
                String out = tsRaw+ "|" + cust + "|" + acc + "|" + ccy + "|" + bal + "\n";

                // 路由到目标分片：用客户号取模，分布较均匀
                int shard = (int)(Long.parseLong(cust) % shards);
                queues[shard].put(out.getBytes(StandardCharsets.UTF_8)); // 阻塞式放入
                ok++;

                if ((ok % 5_000_000) == 0) { // 大文件进度提示
                    log.info("queued {} rows, bad {}", ok, bad);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // 通知写线程：读已结束
            finished[0] = true;
        }

        // 等待所有写线程退出
        writers.shutdown();
        try { writers.awaitTermination(7, TimeUnit.DAYS); } catch (InterruptedException ignored) {}
        log.info("EQUITY LOAD finished. success={} fail={}", ok, bad);

        // 计数落盘，供回调脚本读取
        dumpStat("equity.ok", String.valueOf(ok));
        dumpStat("equity.fail", String.valueOf(bad));
    }

    /**
     * 单个分片的写线程：不断从队列取数据，通过 Pipe 喂给 COPY。
     * @param shard     分片编号（0-based）
     * @param queues    所有分片队列
     * @param finished  读线程结束标志
     */
    private void copyWorker(int shard, ArrayBlockingQueue<byte[]>[] queues, boolean[] finished) {
        String table = tablePrefix + String.format("%02d", shard + 1);
        String copySql = "COPY " + table +
                " (biz_dt, customer_no, account_no, ccy, balance) FROM STDIN WITH (FORMAT text, DELIMITER '|')";

        long total = 0L;
        final int BATCH_SIZE = 50000; // 每批5万行，按需调大调小
        try {
            while (true) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                int count = 0;

                // 从队列取一批
                while (count < BATCH_SIZE) {
                    byte[] line = queues[shard].poll(500, TimeUnit.MILLISECONDS);
                    if (line != null) {
                        baos.write(line);
                        count++;
                    } else if (finished[0] && queues[shard].isEmpty()) {
                        break;
                    }
                }

                if (count > 0) {
                    try (Connection conn = dataSource.getConnection()) {
                        CopyManager cm = new CopyManager(conn.unwrap(BaseConnection.class));
                        long n = cm.copyIn(copySql, new ByteArrayInputStream(baos.toByteArray()));
                        total += n;
                        // 每批打印日志
                        log.info("{} imported batch {} rows, total={}", table, n, total);
                    }
                }

                // 读线程结束 & 队列空，跳出循环
                if (finished[0] && queues[shard].isEmpty()) {
                    break;
                }
            }
            log.info("{} copied rows = {}", table, total);
        } catch (Exception e) {
            throw new RuntimeException("copyWorker " + (shard + 1), e);
        }
    }


    // ======== 基础校验工具 ========
    // ======== 工具 ========
    private static boolean isAllDigits(String s){
        if (s==null || s.isEmpty()) return false;
        for (int i=0;i<s.length();i++) if (!Character.isDigit(s.charAt(i))) return false;
        return true;
    }
    private static boolean isAlpha(String s){
        if (s==null || s.isEmpty()) return false;
        for (int i=0;i<s.length();i++) if (!Character.isLetter(s.charAt(i))) return false;
        return true;
    }
    private static boolean isDecimal(String s){
        try { new java.math.BigDecimal(s); return true; } catch (Exception e){ return false; }
    }
    private static boolean isTs(String s){
        try { LocalDateTime.parse(s, TS); return true; } catch (Exception e){ return false; }
    }
    private static String leftPadToLen(String s, int len){
        if (s.length()>=len) return s;
        StringBuilder sb=new StringBuilder(len);
        for(int i=0;i<len-s.length();i++) sb.append('0');
        return sb.append(s).toString();
    }
    private void dumpStat(String file, String val){
        try {
            Path dir = Paths.get(statDir);
            Files.createDirectories(dir);
            Files.write(dir.resolve(file), (val+"\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e){
            log.warn("write stat {} failed", file, e);
        }
    }
}
