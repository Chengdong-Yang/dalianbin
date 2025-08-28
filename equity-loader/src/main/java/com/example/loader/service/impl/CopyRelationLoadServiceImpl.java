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
 * 单表：tb_customer_relation(manager_no CHAR(7), customer_no CHAR(10))
 * 采用 COPY FROM STDIN 进行高性能导入；边读边校验、边规范化（左补零）、边写入。
 */
@Slf4j
@Service
public class CopyRelationLoadServiceImpl implements CopyRelationLoadService {

    private final DataSource dataSource;

    public CopyRelationLoadServiceImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 是否启用（在 application.yml 里配置 loader.relation.enabled=true 触发） */
    @Value("${loader.relation.enabled:false}")
    private boolean enabled;

    /** 目标表名（如需改名可在 yml 中重写） */
    @Value("${loader.relation.tableName:tb_customer_relation}")
    private String tableName;

    /** 坏行输出后缀名 */
    @Value("${loader.badFileSuffix:.bad}")
    private String badSuffix;

    @Value("${loader.statDir:/tmp/8424227}")
    private String statDir;

    @Override
    public void loadFile() {
        if (!enabled) {
            log.info("Relation loader disabled (loader.relation.enabled=false).");
            return;
        }
        // 目录优先用 FLE_PATH（题面要求），未设置则回退 FILE_PATH
        String dir = System.getenv("FLE_PATH");
        if (dir == null || dir.isEmpty()) {
            dir = System.getenv("FILE_PATH");
        }
        String fname = System.getenv("FILE_NAME_RELATION");
        if (dir == null || fname == null) {
            throw new IllegalStateException("环境变量 FLE_PATH/FILE_PATH 或 FILE_NAME_RELATION 未设置");
        }
        Path input = Paths.get(dir, fname);
        if (!Files.isRegularFile(input)) {
            throw new IllegalStateException("关系文件不存在: " + input);
        }
        Path badPath = Paths.get(dir, fname + badSuffix);

        log.info("开始导入关系文件: {}", input);

        // 建立 Pipe：一边读文件写 pipeOut（仅写合规行），COPY 从 pipeIn 读
        try (Connection conn = dataSource.getConnection();
             BufferedReader br = Files.newBufferedReader(input, StandardCharsets.UTF_8);
             BufferedWriter badOut = Files.newBufferedWriter(
                     badPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            final PipedOutputStream pipeOut = new PipedOutputStream();
            final PipedInputStream  pipeIn  = new PipedInputStream(pipeOut, 8 * 1024 * 1024); // 8MB 缓冲

            final long[] okFail = {0L, 0L};

            Thread feeder = new Thread(() -> {
                long ok = 0, bad = 0;
                try (OutputStream os = pipeOut) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] arr = line.split("[|｜]", -1);
                        if (arr.length != 2) { badOut.write(line); badOut.newLine(); bad++; continue; }
                        String mgrRaw  = arr[0].trim();
                        String custRaw = arr[1].trim();

                        // csmgr_refno：字母数字，1..7
                        if (mgrRaw.isEmpty() || mgrRaw.length() > 7 || !isAlphaNum(mgrRaw)) {
                            badOut.write(line); badOut.newLine(); bad++; continue;
                        }
                        String manager = mgrRaw.toUpperCase();

                        // customer_no：数字、≤10；不足左补0
                        if (!isAllDigits(custRaw) || custRaw.length() > 10) {
                            badOut.write(line); badOut.newLine(); bad++; continue;
                        }
                        String customer = leftPadToLen(custRaw, 10);

                        String out = manager + "|" + customer + "\n";
                        os.write(out.getBytes(StandardCharsets.UTF_8));
                        ok++;
                        if ((ok % 5_000_000) == 0) {
                            log.info("relation feeder progress: ok={} bad={}", ok, bad);
                        }
                    }
                    log.info("relation feeder finished: ok={} bad={}", ok, bad);
                } catch (Exception e) {
                    throw new RuntimeException("relation feeder 异常", e);
                } finally {
                    okFail[0] = ok; okFail[1] = bad;
                }
            }, "relation-feeder");

            feeder.start();

            CopyManager cm = new CopyManager(conn.unwrap(BaseConnection.class));
            String copySql = "COPY " + tableName + " (csmgr_refno, customer_no) " +
                    "FROM STDIN WITH (FORMAT text, DELIMITER '|')";
            long copied = cm.copyIn(copySql, pipeIn);

            feeder.join(TimeUnit.MINUTES.toMillis(120));
            log.info("RELATION COPY 完成，成功行数(流入COPY)：{}（COPY返回={}）", okFail[0], copied);

            dumpStat("relation.ok", String.valueOf(okFail[0]));
            dumpStat("relation.fail", String.valueOf(okFail[1]));
        } catch (Exception e) {
            throw new RuntimeException("关系文件导入失败", e);
        }
    }

    // ========= 工具 =========
    // 工具
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