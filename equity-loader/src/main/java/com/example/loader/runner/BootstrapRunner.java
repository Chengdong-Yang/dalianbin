package com.example.loader.runner;

import com.example.loader.service.CopyLoadService;
import com.example.loader.service.CopyRelationLoadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 启动执行器：
 * - loader.enabled=true           -> 跑资产文件
 * - loader.relation.enabled=true  -> 跑关系文件（本需求）
 * 两者互不影响，可单独/同时打开。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BootstrapRunner implements CommandLineRunner {

    private final CopyLoadService copyLoadService;                // 资产
    private final CopyRelationLoadService copyRelationLoadService; // 关系

    @Value("${loader.enabled:false}")
    private boolean equityEnabled;

    @Value("${loader.relation.enabled:false}")
    private boolean relationEnabled;

    /** 与装载服务里 dumpStat() 一致的目录 */
    @Value("${loader.statDir:/tmp/8424227}")
    private String statDir;

    /** 回调 #1（铺底完成）配置，可被环境变量覆盖 */
    @Value("${callback.cb1-url:http://82.202.169.44:6000/callback1}")
    private String cb1Url;
    @Value("${callback.omr-acc:8424227}")
    private String omrAcc;
    @Value("${callback.omr-pwd:762687ok@}")
    private String omrPwd;
    @Value("${callback.max-retries:3}")
    private int maxRetries;
    @Value("${callback.backoff-ms:1500}")
    private long backoffMs;

    private final RestTemplate http = new RestTemplate();

    @Override
    public void run(String... args) {
        if (!equityEnabled && !relationEnabled) {
            log.info("All loaders disabled. Set loader.enabled or loader.relation.enabled to true.");
            return;
        }
        if (equityEnabled) {
            long t0 = System.currentTimeMillis();
            log.info("Starting EQUITY COPY loader ...");
            copyLoadService.loadFile();
            log.info("EQUITY DONE in {} ms", (System.currentTimeMillis()-t0));
        }
        if (relationEnabled) {
            long t0 = System.currentTimeMillis();
            log.info("Starting RELATION COPY loader ...");
            copyRelationLoadService.loadFile();
            log.info("RELATION DONE in {} ms", (System.currentTimeMillis()-t0));
        }

    // === 两个装载都结束后：读统计文件并回调 ===
    long equityOk      = readCounter("equity.ok");
    long equityFail    = readCounter("equity.fail");
    long relationOk    = readCounter("relation.ok");
    long relationFail  = readCounter("relation.fail");

    log.info("Stats summary -> equity(ok={}, fail={}), relation(ok={}, fail={})",
    equityOk, equityFail, relationOk, relationFail);

    Map<String, Object> payload = buildCallback1Payload(equityOk, equityFail, relationOk, relationFail);
    postWithRetry(cb1Url, payload);
    log.info("Callback1 finished.");
}

    // ---------- helpers ----------
    private long readCounter(String file) {
        try {
            Path p = Paths.get(statDir, file);
            if (!Files.exists(p)) return 0L;
            String s = new String(Files.readAllBytes(p)).trim();
            return s.isEmpty() ? 0L : Long.parseLong(s);
        } catch (Exception e) {
            log.warn("readCounter {} failed: {} (treat as 0)", file, e.toString());
            return 0L;
        }
    }

    private Map<String, Object> buildCallback1Payload(long equityOk, long equityFail,
                                                      long relationOk, long relationFail) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("omracc", omrAcc);
        root.put("omrpwd", omrPwd);

        List<Map<String, String>> filelist = new ArrayList<>();
        filelist.add(fileItem("equity",   equityOk,   equityFail));
        filelist.add(fileItem("relation", relationOk, relationFail));
        root.put("filelist", filelist);
        return root;
    }

    private Map<String, String> fileItem(String name, long ok, long fail) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("filename", name);
        m.put("success", String.valueOf(ok));
        m.put("fail",    String.valueOf(fail));
        return m;
    }

    private void postWithRetry(String url, Object body) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Object> req = new HttpEntity<>(body, h);

        for (int attempt = 1; ; attempt++) {
            try {
                ResponseEntity<String> resp = http.postForEntity(url, req, String.class);
                log.info("callback POST {} -> {} {}", url, resp.getStatusCodeValue(), resp.getBody());
                return;
            } catch (Exception ex) {
                log.warn("callback attempt {} failed: {}", attempt, ex.toString());
                if (attempt >= maxRetries) {
                    log.error("callback give up after {} attempts", attempt);
                    return;
                }
                try {
                    Thread.sleep(backoffMs * attempt);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}