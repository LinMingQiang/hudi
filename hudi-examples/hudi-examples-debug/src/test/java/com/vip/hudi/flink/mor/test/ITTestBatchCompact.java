package com.vip.hudi.flink.mor.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;

import java.util.concurrent.ExecutionException;

public class ITTestBatchCompact  extends TestUtil{
    String tblName = "ITTestVipVersionMapping";
    @org.junit.Test
    public void testWrite() throws ExecutionException, InterruptedException {
        init(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(FILE_SRC_TBL);
        tableEnv.executeSql(FILE_SRC_HUDI_TBL(tblName));
        String insertSql = "insert into file_src_hudi_tbl " +
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
        init(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(FILE_SRC_HUDI_TBL(tblName));
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from  file_src_hudi_tbl"+
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
