package com.example.eqapi.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.math.BigDecimal;
@Data
@AllArgsConstructor
public class CustHoldingItem {
    private String acctNo;
    private String cur;
    private BigDecimal balance;
    private String datetime; // yyyy-MM-dd HH:mm:ss
}
