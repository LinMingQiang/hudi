package com.vip.hudi.flink.dingding.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class ITTest_Allen_kyro  extends TestUtil {
    // 报错，是因为bulk insert 不能用streaming
    String hudiTbl = "create table hudi1(\n" +
            "id int," +
            "comb int," +
            "par date\n" +
            ") partitioned by (par)\n" +
            "with (\n" +
            "    'connector' = 'hudi',\n" +
            "    'table.type' = 'MERGE_ON_READ',\n" +
            "   'write.operation'='bulk_insert'," +
            "'hoodie.datasource.write.recordkey.field'='id'," +
            "    'path' = 'file:///Users/hunter/workspace/hudipr/master-debug/hudi-examples/hudi-examples-debug/target/ITTest_Allen_kyro',\n" +
            "    'write.tasks' = '4')\n";

    @Test
    public void testWrite() throws ExecutionException, InterruptedException {
        initEnv(RuntimeExecutionMode.STREAMING);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.executeSql(hudiTbl);
        tableEnv.executeSql("create table dataagen with( " +
                "'connector' = 'datagen'," +
                "'number-of-rows'='10'" + // batch
//                "'rows-per-second'='10'" + // streaming
                ") like hudi1 (excluding all)");

        tableEnv.executeSql("insert into hudi1 select * from dataagen").await();

    }

    @org.junit.Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);

        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        tableEnv.executeSql(hudiTbl);

        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from hudi1 " +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20230826102700570') */"
                ), Row.class)
                .print();
        env.execute();
    }
}
