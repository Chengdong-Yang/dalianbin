package com.example.mq.consumer;

import com.example.mq.service.EquityService;
import com.example.mq.service.MqMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
public class EquityConsumer {

    private final EquityService equityService;
    private final MqMetrics metrics;
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${pulsar.url}")                     private String pulsarUrl;
    @Value("${pulsar.topic}")                   private String topic;
    @Value("${pulsar.subscription}")            private String subscription;
    @Value("${pulsar.token:}")                  private String token;

    @Value("${pulsar.idle-seconds:30}")         private int idleSeconds;
    @Value("${pulsar.grace-seconds:10}")        private int graceSeconds;
    @Value("${pulsar.receive-timeout-ms:1000}") private long receiveTimeoutMs;

    @Value("${pulsar.callback-url}")            private String callbackUrl;
    @Value("${pulsar.omr-acc}")                 private String omrAcc;
    @Value("${pulsar.omr-pwd}")                 private String omrPwd;

    private PulsarClient client;
    private Consumer<String> consumer;

    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    @PostConstruct
    public void start() throws Exception {
        log.info("Starting Pulsar consumer (single-thread), url={}, topic={}, sub={}, idle={}s grace={}s",
                pulsarUrl, topic, subscription, idleSeconds, graceSeconds);

        // 1) client
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .ioThreads(1)
                .listenerThreads(1);
        if (token != null && !token.isEmpty()) {
            clientBuilder.authentication(AuthenticationFactory.token(token));
        }
        client = clientBuilder.build();

        // 2) consumer
        consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("a")
                .subscribe();

        // 3) 单工作线程
        Thread worker = new Thread(this::workerLoop, "consumer-worker");
        worker.setDaemon(true);
        worker.start();

        // 4) 监控：静默 -> 二次确认 -> 回调 -> 优雅关闭（单线程无 in-flight 并发，判断更简单）
        monitor.scheduleWithFixedDelay(this::maybeCallbackAndShutdown, 5, 5, TimeUnit.SECONDS);
    }

    private void workerLoop() {
        while (!stopping.get()) {
            Message<String> msg = null;
            try {
                msg = consumer.receive((int) receiveTimeoutMs, TimeUnit.MILLISECONDS);
                if (msg == null) continue; // 轮询超时，继续

                metrics.onReceive();
                final String value = msg.getValue();
                log.info("received msgId={}, payload={}", msg.getMessageId(), value);

                boolean shouldAck = equityService.handleMessage(value);
                if (shouldAck) {
                    consumer.acknowledge(msg);
                } else {
                    consumer.negativeAcknowledge(msg);
                }
            } catch (Exception e) {
                log.error("消费异常", e);
                if (msg != null) {
                    try { consumer.negativeAcknowledge(msg); } catch (Exception ignore) {}
                }
            }
        }
        log.info("worker exit");
    }

    /** 静默 → 等待 grace 窗口二次确认仍静默 → 回调并优雅关闭 */
    private void maybeCallbackAndShutdown() {
        if (stopping.get()) return;

        long idle = metrics.secondsSinceLastMsg();
        if (idle < idleSeconds) return;

        final long touch = metrics.lastTouchNanos();
        final long totalBefore = metrics.okCount() + metrics.failCount();
        log.info("idle={}s → 二次确认等待 {}s", idle, graceSeconds);

        try { Thread.sleep(graceSeconds * 1000L); } catch (InterruptedException ignored) {}

        boolean stillIdle = (touch == metrics.lastTouchNanos())
                && totalBefore == (metrics.okCount() + metrics.failCount());

        if (!stillIdle) {
            log.info("二次确认窗口有新消息，继续观察");
            return;
        }

        if (!stopping.compareAndSet(false, true)) return;

        try {
            postCallback(metrics.okCount());
        } catch (Exception e) {
            log.error("callback failed", e);
        }

        try { consumer.close(); } catch (Exception ignore) {}
        try { client.close(); } catch (Exception ignore) {}
        monitor.shutdownNow();
        log.info("consumer shutdown done");
    }

    private void postCallback(long success) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("omracc", omrAcc);
        payload.put("omrpwd", omrPwd);
        payload.put("success", String.valueOf(success)); // 平台需要字符串

        log.info("POST callback: {} payload={}", callbackUrl, payload);
        String resp = restTemplate.postForObject(callbackUrl, payload, String.class);
        log.info("callback response: {}", resp);
    }
}
