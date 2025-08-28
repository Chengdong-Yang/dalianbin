package com.example.loader.runner;

import com.example.loader.service.CopyLoadService;
import com.example.loader.service.CopyRelationLoadService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

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
    }
}