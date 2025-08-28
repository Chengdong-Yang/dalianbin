// service/EquityQueryService.java
package com.example.eqapi.service;

import com.example.eqapi.dto.*;
import com.example.eqapi.mapper.AggDailyMapper;
import com.example.eqapi.mapper.EquityQueryMapper;
import com.example.eqapi.mapper.RelationMapper;
import com.example.eqapi.util.MoneyUtil;
import com.example.eqapi.util.ShardUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import com.example.eqapi.dto.CsmgrDailyResp;
import com.example.eqapi.dto.CsmgrDailyResp.CustAmt;
import com.example.eqapi.dto.CsmgrDailyResp.DayBlock;
import com.example.eqapi.mapper.AggDailyMapper;
import com.example.eqapi.util.MoneyUtil;
import com.example.eqapi.util.ShardUtil;


@Service
@RequiredArgsConstructor
public class EquityQueryService {

  private final EquityQueryMapper equityMapper;
  private final AggDailyMapper aggDailyMapper;
  private final RelationMapper relationMapper;
  private final RateCacheService rateCache; // 你已有的缓存服务：CNY=1，定时刷新

  private static final int SHARDS = 16;
  private static final String SNAPSHOT_PREFIX = "tb_customer_equity_";
  private static final String AGG_PREFIX = "agg_cust_daily_";

  /** ① 单客户资产明细 + 人民币总额 */
  public QueryByCustNoResp queryByCustNo(QueryByCustNoReq req){
    String custNo = ShardUtil.leftPad10(req.getCustNo());
    String suffix = ShardUtil.shardSuffix(custNo);
    String table = SNAPSHOT_PREFIX + suffix;

    List<EquityQueryMapper.HoldingRow> rows =
            equityMapper.selectHoldingsFromTable(table, custNo);

    // 组装明细 & 计算总额（balance × rate），两位小数“舍弃”
    BigDecimal total = BigDecimal.ZERO;
    List<CustHoldingItem> detail = new ArrayList<>();
    for (EquityQueryMapper.HoldingRow r : rows){
      BigDecimal rate = rateCache.getRate(r.ccy); // miss时内部回库一次
      BigDecimal cny = (r.balance==null?BigDecimal.ZERO:r.balance).multiply(rate);
      total = total.add(cny);

      detail.add(new CustHoldingItem(
              r.accountNo, r.ccy, r.balance==null?BigDecimal.ZERO:r.balance, r.bizDt
      ));
    }
    QueryByCustNoResp resp = new QueryByCustNoResp();
    resp.setCustNo(custNo);
    resp.setTotalAmt(MoneyUtil.truncate2(total));
    resp.setDetail(detail);
    return resp;
  }

  /** ② 区间客户总资产（并行扫16张快照分表） */
  public RangeTotalResp queryByCustNoRange(QueryByCustNoRangeReq req){
    String min = ShardUtil.leftPad10(req.getCustNoMin());
    String max = ShardUtil.leftPad10(req.getCustNoMax());

    // 并行汇总每个分表的 SUM(balance×rate)
    BigDecimal total = IntStream.rangeClosed(1, SHARDS).parallel()
            .mapToObj(i -> {
              String suffix = String.format("%02d", i);
              String table = SNAPSHOT_PREFIX + suffix;
              BigDecimal part = equityMapper.sumRangeFromTable(table, min, max);
              return part==null?BigDecimal.ZERO:part;
            })
            .reduce(BigDecimal.ZERO, BigDecimal::add);

    RangeTotalResp resp = new RangeTotalResp();
    resp.setTotalAmt(MoneyUtil.truncate2(total));
    return resp;
  }

  /** ③ 客户经理名下每日动账净变动（人民币）；返回按日期分组的客户列表 */
  public CsmgrDailyResp queryByCsmgrRange(QueryByCsmgrReq req){
    String mgr = req.getCsmgrRefno();
    List<String> allCusts = relationMapper.findCustByMgr(mgr);
    CsmgrDailyResp resp = new CsmgrDailyResp();

    if (allCusts == null || allCusts.isEmpty()) {
      resp.setDateList(java.util.Collections.emptyList());
      return resp;
    }

    // 1) 按分片拆客户集合
    Map<String, List<String>> shardMap = new java.util.HashMap<>();
    for (String c : allCusts){
      String c10 = ShardUtil.leftPad10(c);
      String suf = ShardUtil.shardSuffix(c10);
      shardMap.computeIfAbsent(suf, k -> new java.util.ArrayList<>()).add(c10);
    }

    // 2) 拉取每个分片的 (ymd, customer_no, amount_cny) 明细，按日期聚合客户列表
    Map<String, List<CustAmt>> dateToCusts = new java.util.HashMap<>();
    shardMap.forEach((suf, custList) -> {
      String table = AGG_PREFIX + suf; // agg_cust_daily_XX
      List<AggDailyMapper.CustDayRow> rows =
              aggDailyMapper.listManagerDailyPerCustFromTable(table, custList);

      for (AggDailyMapper.CustDayRow r : rows){
        // 两位小数“舍弃”
        java.math.BigDecimal amt = MoneyUtil.truncate2(r.amount == null ? java.math.BigDecimal.ZERO : r.amount);
        if (amt.signum() == 0) continue; // 可选：金额为 0 的日记录可忽略

        List<CustAmt> list = dateToCusts.computeIfAbsent(r.ymd, k -> new java.util.ArrayList<>());
        list.add(new CustAmt(r.custNo, amt));
      }
    });

    // 3) 排序：日期升序、同日内按客户号升序；封装为 dateList
    java.util.List<DayBlock> dateList = dateToCusts.entrySet().stream()
            .sorted(java.util.Map.Entry.comparingByKey()) // yyyymmdd 升序
            .map(e -> {
              List<CustAmt> custs = e.getValue();
              custs.sort(java.util.Comparator.comparing(CustAmt::getCustNo));
              return new DayBlock(e.getKey(), custs);
            })
            .collect(java.util.stream.Collectors.toList());

    resp.setDateList(dateList);
    return resp;
  }
}