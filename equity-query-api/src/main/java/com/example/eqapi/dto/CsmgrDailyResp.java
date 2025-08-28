// dto/CsmgrDailyResp.java
package com.example.eqapi.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.math.BigDecimal;
import java.util.List;

@Data
public class CsmgrDailyResp {
    private List<DayBlock> dateList;

    @Data @AllArgsConstructor
    public static class DayBlock {
        private String date;              // YYYYMMDD
        private List<CustAmt> custList;  // 当天每个客户的净变动（人民币，两位小数“舍弃”）
    }

    @Data @AllArgsConstructor
    public static class CustAmt {
        private String custNo;
        private BigDecimal amount;
    }
}