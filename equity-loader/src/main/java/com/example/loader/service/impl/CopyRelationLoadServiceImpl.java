package com.example.loader.service.impl;

import com.example.loader.service.CopyRelationLoadService;
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
import java.util.concurrent.TimeUnit;

/**
 * 关系表（tb_customer_relation）的 COPY 装载：
 * - 单连接复用（解决长 Pipe 连接易断问题）
 * - 批量 COPY：每 50,000 行 flush 一次，并打印进度
 * - 字段校验：manager(<=7 位字母数字)、customer(<=10 位数字，不足左补0)
 * - 坏行写 *.bad，统计 relation.ok / relation.fail 到 ${loader.statDir}
 */
@Slf4j
@Service
public class CopyRelationLoadServiceImpl implements CopyRelationLoadService {

    private final DataSource dataSource;
    public CopyRelationLoadServiceImpl(DataSource dataSource) { this.dataSource = dataSource; }

    @Value("${loader.relation.enabled:false}")
    private boolean enabled;

    @Value("${loader.relation.tableName:tb_customer_relation}")
    private String tableName;

    @Value("${loader.badFileSuffix:.bad}")
    private String badSuffix;

    @Value("${loader.statDir:/tmp/8424227}")
    private String statDir;

    /** 每批 COPY 行数阈值（到达即 flush） */
    @Value("${loader.relation.batchSize:50000}")
    private int batchSize;

    @Override
    public void loadFile() {
        if (!enabled) {
            log.info("Relation loader disabled (loader.relation.enabled=false).");
            return;
        }

        // 题面：优先 FLE_PATH（如果平台给错变量名），否则 FILE_PATH
        String dir = System.getenv("FLE_PATH");
        if (dir == null || dir.isEmpty()) dir = System.getenv("FILE_PATH");
        String fname = System.getenv("FILE_NAME_RELATION");
        if (dir == null || fname == null) {
            throw new IllegalStateException("环境变量 FLE_PATH/FILE_PATH 或 FILE_NAME_RELATION 未设置");
        }

        Path input = Paths.get(dir, fname);
        if (!Files.isRegularFile(input)) throw new IllegalStateException("关系文件不存在: " + input);
        Path badPath = Paths.get(dir, fname + badSuffix);

        log.info("开始导入关系文件: {}", input);

        long ok = 0, bad = 0;
        long totalCopied = 0;
        int buffered = 0;

        // 复用单一连接与 CopyManager
        try (Connection conn = dataSource.getConnection();
             BufferedReader br = Files.newBufferedReader(input, StandardCharsets.UTF_8);
             BufferedWriter badOut = Files.newBufferedWriter(
                     badPath, StandardCharsets.UTF_8,
                     StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            CopyManager cm = new CopyManager(conn.unwrap(BaseConnection.class));
            final String copySql = "COPY " + tableName + " (csmgr_refno, customer_no) FROM STDIN WITH (FORMAT text, DELIMITER '|')";

            // 用内存缓冲聚一批再 COPY（防止超长管道/网络断开）
            ByteArrayOutputStream batch = new ByteArrayOutputStream(8 * 1024 * 1024); // 8MB 初始

            String line;
            while ((line = br.readLine()) != null) {
                String[] arr = line.split("[|｜]", -1);
                if (arr.length != 2) { badOut.write(line); badOut.newLine(); bad++; continue; }

                String mgrRaw  = arr[0].trim();
                String custRaw = arr[1].trim();

                // manager：1..7 位，字母数字
                if (mgrRaw.isEmpty() || mgrRaw.length() > 7 || !isAlphaNum(mgrRaw)) {
                    badOut.write(line); badOut.newLine(); bad++; continue;
                }
                String manager = mgrRaw.toUpperCase();

                // customer：数字，<=10；不足左补0
                if (!isAllDigits(custRaw) || custRaw.length() > 10) {
                    badOut.write(line); badOut.newLine(); bad++; continue;
                }
                String customer = leftPadToLen(custRaw, 10);

                // 写入 batch（一行）
                String out = manager + "|" + customer + "\n";
                batch.write(out.getBytes(StandardCharsets.UTF_8));
                ok++; buffered++;

                // 达到批次阈值 -> flush
                if (buffered >= batchSize) {
                    long n = cm.copyIn(copySql, new ByteArrayInputStream(batch.toByteArray()));
                    totalCopied += n;
                    log.info("RELATION batch copied: {} rows (this), {} rows (total), ok={}, bad={}",
                            n, totalCopied, ok, bad);
                    batch.reset();
                    buffered = 0;
                }

                if ((ok % 5_000_000) == 0) {
                    log.info("relation feeder progress: ok={} bad={}", ok, bad);
                }
            }

            // flush 最后一批
            if (buffered > 0) {
                long n = cm.copyIn(copySql, new ByteArrayInputStream(batch.toByteArray()));
                totalCopied += n;
                log.info("RELATION final batch copied: {} rows (this), {} rows (total)", n, totalCopied);
            }

            log.info("RELATION COPY 完成：ok={} bad={} totalCopied={}", ok, bad, totalCopied);

            // 写统计文件供回调读取
            dumpStat("relation.ok", String.valueOf(ok));
            dumpStat("relation.fail", String.valueOf(bad));

        } catch (Exception e) {
            throw new RuntimeException("关系文件导入失败", e);
        }
    }

    // ===== 工具 =====
    private static boolean isAllDigits(String s) {
        if (s == null || s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) if (!Character.isDigit(s.charAt(i))) return false;
        return true;
    }

    private static boolean isAlphaNum(String s){
        if (s==null || s.isEmpty()) return false;
        for (int i=0;i<s.length();i++) {
            char c = s.charAt(i);
            if (!Character.isLetterOrDigit(c)) return false;
        }
        return true;
    }

    private static String leftPadToLen(String s, int len) {
        if (s.length()>=len) return s;
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len - s.length(); i++) sb.append('0');
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