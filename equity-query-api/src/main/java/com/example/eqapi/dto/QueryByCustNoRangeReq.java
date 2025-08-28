package com.example.eqapi.dto;

import lombok.Data;
@Data
public class QueryByCustNoRangeReq {
    private String custNoMin;
    private String custNoMax;
}
