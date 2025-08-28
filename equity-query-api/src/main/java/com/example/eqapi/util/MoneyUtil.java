package com.example.eqapi.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
public class MoneyUtil {
    /** 两位小数向下取整（舍弃） */
    public static BigDecimal truncate2(BigDecimal v){
        if (v==null) return BigDecimal.ZERO;
        return v.setScale(2, RoundingMode.DOWN);
    }
}