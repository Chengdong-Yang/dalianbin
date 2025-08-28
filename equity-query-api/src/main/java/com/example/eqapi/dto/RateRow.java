package com.example.eqapi.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class RateRow {
    public String ccy;
    public BigDecimal rate;
}