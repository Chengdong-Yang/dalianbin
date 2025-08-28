package com.example.loader.config;

import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.DynamicTableNameInnerInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Function;

/**
 * MyBatis-Plus 的通用配置。
 *
 * 这里启用了「动态表名」插件：
 * - 由于我们对 tb_customer_equity 做了分表（tb_customer_equity_01 ~ tb_customer_equity_16），
 *   在使用 MyBatis-Plus 进行查询/写入时，需要把逻辑表名替换为“真实分表名”。
 * - DynamicTableNameInnerInterceptor 可以在运行时将 SQL 中出现的逻辑表名替换为真实表名。
 * - 替换规则由 TableRouteContext 提供（通过 ThreadLocal 在当前线程传入真实表名）。
 */
@Configuration
public class MybatisPlusConfig {

    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();

        // 动态表名拦截器
        DynamicTableNameInnerInterceptor dynamic = new DynamicTableNameInnerInterceptor();

        // 全局表名处理：如果当前线程设置了真实表名，则替换；否则保持不变
        dynamic.setTableNameHandler((sql, tableName) -> {
            String real = TableRouteContext.getRealTableName();
            return (real == null) ? tableName : real;
        });

        // 也可以只对某些逻辑表生效（可选，二选一）
        // dynamic.setTableNameHandler((sql, tableName) -> {
        //     if ("tb_customer_equity".equalsIgnoreCase(tableName)) {
        //         String real = TableRouteContext.getRealTableName();
        //         return (real == null) ? tableName : real;
        //     }
        //     return tableName;
        // });

        interceptor.addInnerInterceptor(dynamic);
        return interceptor;
    }

    /**
     * 真实表名的解析器：
     * - 若当前线程未指定真实表名（例如普通无分表的表），则直接返回原表名。
     * - 若指定了，则优先返回真实表名。
     */
    static class DynamicNameHandler implements Function<String, String> {
        @Override
        public String apply(String tableName) {
            String real = TableRouteContext.getRealTableName();
            return real == null ? tableName : real;
        }
    }
}
