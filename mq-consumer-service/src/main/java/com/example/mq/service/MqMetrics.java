package com.example.mq.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class MqMetrics {
    private final AtomicLong ok = new AtomicLong();
    private final AtomicLong fail = new AtomicLong();
    private volatile long lastMsgNanos = System.nanoTime();

    private final Path okFile;
    private final Path failFile;

    public MqMetrics(@Value("${app.stat-dir:/tmp/8424227}") String dir){
        try { Files.createDirectories(Paths.get(dir)); } catch (Exception ignore){}
        this.okFile = Paths.get(dir, "mq.ok");
        this.failFile = Paths.get(dir, "mq.fail");
    }

    public void incOk(){ dump(okFile, ok.incrementAndGet()); touch(); }
    public void incFail(){ dump(failFile, fail.incrementAndGet()); touch(); }
    public void onReceive(){ touch(); }

    public long okCount(){ return ok.get(); }
    public long failCount(){ return fail.get(); }

    public long secondsSinceLastMsg(){
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - lastMsgNanos);
    }
    // 新增：用于二次确认时比对是否出现新消息
    public long lastTouchNanos() { return lastMsgNanos; }

    private void touch(){ lastMsgNanos = System.nanoTime(); }

    private void dump(Path p, long v){
        try {
            Files.write(p, (v+"\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (Exception e){ log.warn("write metrics failed: {}", p, e); }
    }
}
