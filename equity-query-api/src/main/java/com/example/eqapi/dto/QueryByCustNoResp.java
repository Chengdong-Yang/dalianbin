package com.example.eqapi.dto;
import com.example.eqapi.dto.CustHoldingItem;
import lombok.Data;
import java.math.BigDecimal;
import java.util.List;
@Data
public class QueryByCustNoResp {
    private String custNo;
    private BigDecimal totalAmt;   // 人民币，两位小数“舍弃”
    private List<CustHoldingItem> detail;
}