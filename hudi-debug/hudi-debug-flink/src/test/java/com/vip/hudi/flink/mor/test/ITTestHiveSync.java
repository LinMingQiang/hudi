package com.vip.hudi.flink.mor.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;

import java.util.concurrent.ExecutionException;

public class ITTestHiveSync extends TestUtil {
    String tblName = "ITTestHiveSync";
    @org.junit.Test
    public void testWrite() throws ExecutionException, InterruptedException {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(FILE_SRC_TBL);
        tableEnv.executeSql(HUDI_MOR_TBL(tblName));
        String insertSql = "insert into HUDI_MOR_TBL /*+ OPTIONS(" +
                "    'hive_sync.enable' = 'true',\n" +
                "    'hive_sync.mode' = 'hms',\n" +
                "    'hive_sync.metastore.uris' = 'thrift://localhost:9083'" + //bigdata-hiveserver.vip.vip.com
//                "    'hive_sync.db' = 'test',\n" +
//                "    'hive_sync.table' = 'ITTestHiveSync',"+
//                "    'hive_sync.table.strategy'='ro'," +
                ") */" +
                " select " +
                "id," +
                "name," +
                "age," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd HH:mm:ss') as rowtime," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd') as dt" +
                " from file_src_tbl";
        tableEnv.executeSql(insertSql).print();
    }

    @org.junit.Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(HUDI_MOR_TBL(tblName));
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from HUDI_MOR_TBL " +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'write.precombine' = 'true',"+
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20240621170750480') */"), Row.class)
                .print();
        env.execute().getJobExecutionResult().wait();
    }
}
