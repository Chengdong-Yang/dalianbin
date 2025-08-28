package com.example.loader.service;

/**
 * 客户-客户经理关系 COPY 装载服务。
 * 文件字段顺序：经理工号|客户号
 * 分隔符：支持半角竖线 '|' 与全角竖线 '｜'
 * 环境变量：
 *  - FLE_PATH（题面写法）或 FILE_PATH（二选一，优先 FLE_PATH）
 *  - FILE_NAME_RELATION
 */
public interface CopyRelationLoadService {
    void loadFile();
}