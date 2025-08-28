package com.example.mq.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;

@Mapper
public interface DailyAggMapper {

    @Insert({
            "<script>",
            "INSERT INTO ${table}(ymd, customer_no, amount_cny) ",
            "VALUES(#{ymd}, #{custNo}, #{delta}) ",
            "ON CONFLICT (ymd, customer_no) ",
            "DO UPDATE SET amount_cny = ${table}.amount_cny + EXCLUDED.amount_cny",
            "</script>"
    })
    void upsertAdd(@Param("table") String table,
                   @Param("ymd") String ymd,
                   @Param("custNo") String custNo,
                   @Param("delta") BigDecimal delta);
}