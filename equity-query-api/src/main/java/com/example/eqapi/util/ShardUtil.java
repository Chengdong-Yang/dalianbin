package com.example.eqapi.util;

public class ShardUtil {
    /** 客户号按数字取模，返回 "01".."16" */
    public static String shardSuffix(String custNo) {
        String s = leftPad10(custNo);
        int n = Integer.parseInt(s);
        int idx = (n % 16) + 1;
        return String.format("%02d", idx);
    }
    public static String leftPad10(String custNo){
        if (custNo==null) return null;
        String s = custNo.trim();
        if (s.length()>=10) return s;
        StringBuilder sb = new StringBuilder(10);
        for (int i=0;i<10-s.length();i++) sb.append('0');
        return sb.append(s).toString();
    }
}