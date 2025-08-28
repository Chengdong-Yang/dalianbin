package com.example.eqapi.dto;

import lombok.Data;
import java.math.BigDecimal;
@Data
public class RangeTotalResp {
    private BigDecimal totalAmt; // 人民币，两位小数“舍弃”
}
