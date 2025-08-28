package com.example.mq.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface InboxMapper {
    @Insert("INSERT INTO mq_inbox(tx_id) VALUES(#{txId}) ON CONFLICT DO NOTHING")
    boolean tryInsert(@Param("txId") String txId);
}
