package com.example.loader.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.loader.entity.Equity;
import org.apache.ibatis.annotations.Mapper;

/**
 * 资产表的 Mapper。
 * - 继承 BaseMapper 后，可直接使用 MyBatis-Plus 的通用 CRUD 方法。
 * - 注意：如果表是分表，使用前通过 TableRouteContext 指定真实表名。
 */
@Mapper
public interface EquityMapper extends BaseMapper<Equity> { }
