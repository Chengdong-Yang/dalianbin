package com.example.eqapi.dto;

import lombok.Data;
@Data
public class QueryByCustNoReq {
    private String custNo; // 已左补0的10位；若调用方未补零，后面Service会补
}
