package com.example.mq.model;

import lombok.Data;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.math.BigDecimal;

@Data
public class TradeMessage {
    private LocalDateTime bizDt;
    private String txId;
    private String customerNo;
    private String accountNo;
    private String cdFlag;
    private String ccy;
    private double amount;
    private double balance;

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static TradeMessage parse(String raw) {
        String[] parts = raw.split("\\|", -1);
        if (parts.length != 8) throw new IllegalArgumentException("field_count!=8");

        // 1) 时间：长度=19
        String ts = parts[0].trim();
        if (ts.length()!=19) throw new IllegalArgumentException("bizDt.length");
        LocalDateTime bizDt;
        try { bizDt = LocalDateTime.parse(ts, TS); }
        catch (Exception e){ throw new IllegalArgumentException("bizDt.format"); }

        // 2) 流水号：<=32，字母数字
        String txId = parts[1].trim();
        if (txId.isEmpty() || txId.length()>32 || !isAlphaNum(txId)) throw new IllegalArgumentException("txId");

        // 3) 客户号：<=10，纯数字；不足左补0到10
        String cust = parts[2].trim();
        if (!isDigits(cust) || cust.length()>10) throw new IllegalArgumentException("customerNo");
        cust = leftPad(cust, 10);

        // 4) 账号：<=18，纯数字；不补0
        String acct = parts[3].trim();
        if (!isDigits(acct) || acct.length()>18) throw new IllegalArgumentException("accountNo");

        // 5) 借贷：C/D
        String cd = parts[4].trim().toUpperCase();
        if (!(cd.equals("C") || cd.equals("D"))) throw new IllegalArgumentException("cdFlag");

        // 6) 币种：3位字母
        String ccy = parts[5].trim().toUpperCase();
        if (ccy.length()!=3 || !isAlpha(ccy)) throw new IllegalArgumentException("ccy");

        // 7) 金额
        double amt;
        try {
            String amountStr = parts[6].trim();
            // 校验格式：整数最多18位，小数最多2位
            if (!amountStr.matches("^-?\\d{1,18}(\\.\\d{1,2})?$")) {
                throw new IllegalArgumentException("amount格式错误：整数最多18位，小数最多2位");
            }
            amt = new BigDecimal(amountStr).doubleValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("amount解析失败: " + e.getMessage());
        }

        // 8) 后余额
        double bal;
        try {
            String balanceStr = parts[7].trim();
            // 校验格式：整数最多18位，小数最多2位
            if (!balanceStr.matches("^-?\\d{1,18}(\\.\\d{1,2})?$")) {
                throw new IllegalArgumentException("balance格式错误：整数最多18位，小数最多2位");
            }
            bal = new BigDecimal(balanceStr).doubleValue();
        } catch (Exception e) {
            throw new IllegalArgumentException("balance解析失败: " + e.getMessage());
        }

        TradeMessage m = new TradeMessage();
        m.setBizDt(bizDt);
        m.setTxId(txId);
        m.setCustomerNo(cust);
        m.setAccountNo(acct);
        m.setCdFlag(cd);
        m.setCcy(ccy);
        m.setAmount(amt);
        m.setBalance(bal);
        return m;
    }

    private static boolean isDigits(String s){ if (s==null||s.isEmpty())return false; for(int i=0;i<s.length();i++) if(!Character.isDigit(s.charAt(i)))return false; return true; }
    private static boolean isAlpha(String s){ if (s==null||s.isEmpty())return false; for(int i=0;i<s.length();i++) if(!Character.isLetter(s.charAt(i)))return false; return true; }
    private static boolean isAlphaNum(String s){ if (s==null||s.isEmpty())return false; for(int i=0;i<s.length();i++){char c=s.charAt(i); if(!Character.isLetterOrDigit(c))return false;} return true; }
    private static String leftPad(String s,int len){ if(s.length()>=len)return s; StringBuilder sb=new StringBuilder(len); for(int i=0;i<len-s.length();i++)sb.append('0'); return sb.append(s).toString(); }
}