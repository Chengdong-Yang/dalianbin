package com.example.mq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class MqConsumerServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqConsumerServiceApplication.class, args);
        System.out.println("start");
    }
}
