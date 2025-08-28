package com.example.eqapi.controller;

import com.example.eqapi.dto.*;
import com.example.eqapi.service.EquityQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequiredArgsConstructor
public class EquityQueryController {

  private final EquityQueryService service;

  // 2.3.1 客户资产查询
  @PostMapping("/queryAmtByCustNo")
  public QueryByCustNoResp queryAmtByCustNo(@RequestBody QueryByCustNoReq req){
    return service.queryByCustNo(req);
  }

  // 2.3.2 多客户资产统计
  @PostMapping("/queryAmtByCustNoRange")
  public RangeTotalResp queryAmtByCustNoRange(@RequestBody QueryByCustNoRangeReq req){
    return service.queryByCustNoRange(req);
  }

  // 2.3.3 客户经理名下每日动账统计（返回仅有交易的日期）
  @PostMapping("/queryAmtByCsmgrRange")
  public CsmgrDailyResp queryAmtByCsmgrRange(@RequestBody QueryByCsmgrReq req){
    return service.queryByCsmgrRange(req);
  }
}