package com.example.loader.entity;

import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 客户资产实体（与数据文件一一对应）。
 * 字段说明：
 * - bizDt       交易日期时间（格式：yyyy-MM-dd HH:mm:ss）
 * - customerNo  客户号（10 位数字，文件中不足左补 0）
 * - accountNo   账号（18 位数字）
 * - ccy         币种（3 位英文，如 CNY）
 * - balance     余额（示例 1000.29，建议 NUMERIC(21,2)）
 *
 * 说明：首轮铺底推荐使用 COPY，不强制用该实体对象逐行插入；
 *      该实体更适合后续日常增量或查询返回值。
 */
@Data
public class Equity {
    private LocalDateTime bizDt;
    private String customerNo;
    private String accountNo;
    private String ccy;
    private BigDecimal balance;
}
