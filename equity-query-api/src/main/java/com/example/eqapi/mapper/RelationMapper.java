package com.example.eqapi.mapper;


import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import java.util.List;

@Mapper
public interface RelationMapper {
    List<String> findCustByMgr(@Param("csmgrRefno") String csmgrRefno);
}
