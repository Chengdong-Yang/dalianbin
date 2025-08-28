package com.example.mq.consumer;

import com.example.mq.service.EquityService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;


@Slf4j
@Component
public class EquityConsumer {

    public EquityConsumer(EquityService equityService) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650") // 与 producer 一致
                .build();

        String topic = "persistent://public/default/skill_competition";
        String subName = System.getenv().getOrDefault("SUBSCRIPTION_NAME", "equity-sub-test1"); // 换个新订阅名

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest) // 关键：从最早位置开始
//                .ackTimeout(java.time.Duration.ofMinutes(2)) // 可选：防止一直不 ack 堆积
                .subscribe();

        log.info("Pulsar consumer started. topic={}, sub={}, type={}, initial=Earliest", topic, subName, "Shared");

        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                try {
                    Message<String> msg = consumer.receive(); // 阻塞等待
                    String value = msg.getValue();
                    log.info("received msgId={}, payload={}", msg.getMessageId(), value);
                    equityService.handleMessage(value);
                    consumer.acknowledge(msg);
                } catch (Exception e) {
                    log.error("处理消息失败，将重投递", e);
                }
            }
        });
    }
}
