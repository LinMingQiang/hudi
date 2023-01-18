package com.vip.hudi.flink.mor.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class ITTestKafkaSrc extends TestUtil {
    String pathName = "ITTestVipVersionMapping";

    @Test
    public void testReadSrc() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(KAFKA_SRC_TBL);
        tableEnv.toAppendStream(tableEnv.sqlQuery(
                                "select name from kafka_src_tbl" +
                                        "/*+ OPTIONS(" +

// 1660816847000 有数据。
// 1670819307000 超过最后一条数据
//" 'scan.startup.mode' = 'timestamp',\n" +
//" 'scan.startup.timestamp-millis'='1670819307000'\n" +
//
                        "    'scan.startup.mode' = 'batch',\n" +
                        "    'scan.startup.timestamp-millis'='0',\n" +
                        "    'scan.end.timestamp-millis'='1760809660000'" +

                                        ") */"),
                        Row.class)
                .print();
        env.execute();

    }

    @Test
    public void testWrite() throws ExecutionException, InterruptedException {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(KAFKA_SRC_TBL);
        tableEnv.executeSql(HUDI_MOR_TBL(pathName));
        String insertSql = "insert into HUDI_MOR_TBL /*+ OPTIONS(" +
                "    'hoodie.table.version.fields' = 'dt,hm', \n" +
                "    'hoodie.table.version.values' = '2022-01-01,1100'\n" +
                ") */" +
                " select " +
                "id," +
                "name," +
                "age," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd HH:mm:ss') as rowtime," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd') as dt" +
                " from kafka_src_tbl " +
                "/*+ OPTIONS(" +
                "'scan.startup.mode'='timestamp'," +
                "'scan.startup.timestamp-millis'='1660809660000') */";
        tableEnv.executeSql(insertSql).await();
    }


    @Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(HUDI_MOR_TBL(pathName));
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from HUDI_MOR_TBL " +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'write.precombine' = 'true'," +
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20240621170750480') */"), Row.class)
                .print();
        env.execute();
    }
}
