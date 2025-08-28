package com.example.loader.config;

/**
 * 表路由上下文（基于 ThreadLocal）：
 * - 在一次 MyBatis 操作中，通过 TableRouteContext.use(realTableName, runnable) 指定真实表名，
 *   拦截器会把 SQL 中的逻辑表名替换为这里提供的真实表名。
 * - 使用 ThreadLocal 的原因：避免在方法参数中层层传递表名，且线程隔离安全。
 */
public class TableRouteContext {
    private static final ThreadLocal<String> TL = new ThreadLocal<>();

    /**
     * 在当前线程中使用指定的真实表名执行一个操作。
     * @param realTable 真实表名，例如 "tb_customer_equity_03"
     * @param run       要执行的逻辑（通常是一次 MyBatis 的 CRUD 调用）
     */
    public static void use(String realTable, Runnable run) {
        try {
            TL.set(realTable);
            run.run();
        } finally {
            TL.remove();
        }
    }

    /** 供拦截器读取真实表名 */
    public static String getRealTableName() {
        return TL.get();
    }
}
