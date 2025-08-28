package com.example.mq.service;

import com.example.mq.mapper.RateMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class RateCacheService {

  private final RateMapper rateMapper;
  // 读多写少，用 ConcurrentHashMap 足够；也便于 miss 后回填
  private final ConcurrentHashMap<String, BigDecimal> cache = new ConcurrentHashMap<>();

  public RateCacheService(RateMapper rateMapper) {
    this.rateMapper = rateMapper;
  }

  /** 应用启动：全量预热一次 */
  @PostConstruct
  public void init() {
    try {
      List<RateMapper.RateRow> all = rateMapper.findAll();
      for (RateMapper.RateRow r : all) {
        if (r != null && r.ccy != null && r.rate != null) {
          cache.put(r.ccy.toUpperCase(), r.rate);
        }
      }
      // 兜底：CNY 必须为 1
      cache.put("CNY", BigDecimal.ONE);
      log.info("FX cache preloaded: {} currencies", cache.size());
    } catch (Exception e) {
      // 启动期加载失败不终止应用：后续 getRate 会按需回源数据库
      log.warn("FX cache preload failed, will lazy-load on misses.", e);
      cache.putIfAbsent("CNY", BigDecimal.ONE);
    }
  }

  /** 获取汇率（CNY=1；miss 时回源数据库并回填；未知币种返回 0） */
  public BigDecimal getRate(String ccy) {
    if (ccy == null) return BigDecimal.ZERO;
    if ("CNY".equalsIgnoreCase(ccy)) return BigDecimal.ONE;

    final String key = ccy.trim().toUpperCase();
    // 先查缓存
    BigDecimal v = cache.get(key);
    if (v != null) return v;

    // 缓存未命中：回源数据库，并回填（computeIfAbsent 避免并发重复查库）
    return cache.computeIfAbsent(key, k -> {
      try {
        BigDecimal db = rateMapper.findRate(k);
        if (db == null) {
          log.warn("FX rate not found for {}", k);
          return BigDecimal.ZERO; // 未知币种
        }
        return db;
      } catch (Exception e) {
        log.warn("FX DB lookup failed for {}", k, e);
        return BigDecimal.ZERO;
      }
    });
  }
}