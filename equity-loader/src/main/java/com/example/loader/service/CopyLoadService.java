package com.example.loader.service;

/**
 * COPY 装载服务接口：
 * - 负责将大文件（UTF-8，字段分隔符 | ）并发装入多个分表。
 * - 基于 GaussDB（PG 兼容）的 CopyManager，性能远优于循环 INSERT。
 */
public interface CopyLoadService {
    /** 执行一次装载（读取环境变量 FILE_PATH / FILE_NAME_EQUITY） */
    void loadFile();
}
