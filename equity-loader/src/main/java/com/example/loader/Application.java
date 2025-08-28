package com.example.loader;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 应用入口。
 * - 使用 Spring Boot 启动。
 * - 如果在 application.yml 中将 loader.enabled=true，启动后会自动执行数据铺底装载流程。
 */
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
