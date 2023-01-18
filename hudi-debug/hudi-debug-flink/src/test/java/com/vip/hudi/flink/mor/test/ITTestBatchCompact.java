package com.vip.hudi.flink.mor.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;

import java.util.concurrent.ExecutionException;

public class ITTestBatchCompact  extends TestUtil {
    public static String TABLE_NAME = "ITTestBatchCompact";
    @org.junit.Test
    public void testWrite() throws ExecutionException, InterruptedException {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(FILE_SRC_TBL);
        tableEnv.executeSql(HUDI_MOR_TBL(TABLE_NAME));
        String insertSql = "insert into HUDI_MOR_TBL " +
                " select " +
                "id," +
                "name," +
                "age," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd HH:mm:ss') as rowtime," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd') as dt" +
                " from file_src_tbl";

        tableEnv.executeSql(insertSql).await();
    }

    @org.junit.Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(HUDI_MOR_TBL(TABLE_NAME));
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from  HUDI_MOR_TBL"+
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'write.precombine' = 'true',"+
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20240621170750480') */"), Row.class)
                .print();
        env.execute();
    }
}
