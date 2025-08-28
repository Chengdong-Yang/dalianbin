package com.example.mq.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class MqMetrics {
    private final AtomicLong ok = new AtomicLong();
    private final AtomicLong fail = new AtomicLong();
    private final Path okFile;
    private final Path failFile;

    public MqMetrics(@Value("${app.stat-dir:/tmp/8424227}") String dir){
        try { Files.createDirectories(Paths.get(dir)); } catch (Exception ignore){}
        this.okFile = Paths.get(dir, "mq.ok");
        this.failFile = Paths.get(dir, "mq.fail");
    }

    public void incOk(){ dump(okFile, ok.incrementAndGet()); }
    public void incFail(){ dump(failFile, fail.incrementAndGet()); }

    private void dump(Path p, long v){
        try {
            Files.write(p, (v+"\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e){ log.warn("write metrics failed: {}", p, e); }
    }
}