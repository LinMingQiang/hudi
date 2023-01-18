package com.vip.hudi.flink.mor.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

public class ITTest4726 extends TestUtil {
    String ddl = "CREATE TABLE hudi_4726(" +
            "                                     id string,\n" +
            "                                     msg string,\n" +
            "                                     `partition` STRING,\n" +
            "                                     PRIMARY KEY(id) NOT ENFORCED\n" +
            ") PARTITIONED BY (`partition`)\n" +
            "    WITH (\n" +
            "        'connector' = 'hudi',\n" +
            "        'write.operation'='upsert',\n" +
            "        'path' = 'file:///Users/hunter/workspace/hudipr/master-debug/hudi-examples/hudi-examples-debug/target/hudi_4726',\n" +
            "        'index.type' = 'BUCKET',\n" +
            "        'hoodie.bucket.index.num.buckets' = '2'," +
            "        'compaction.delta_commits' = '2'," +
            "        'table.type' = 'MERGE_ON_READ'," +
            "        'compaction.async.enabled'='true'" +
            ")";
    @Test
    public void testtWrite() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        tableEnv.executeSql(ddl);
        System.out.println(ddl);
        tableEnv.executeSql("INSERT INTO hudi_4726 values ('id1','t1','par1')").await();
        tableEnv.executeSql("INSERT INTO hudi_4726 values ('id1','t2','par1')").await();
        tableEnv.executeSql("INSERT INTO hudi_4726 values ('id1','t3','par1')").await();
        tableEnv.executeSql("INSERT INTO hudi_4726 values ('id1','t4','par1')").await();

    }

    @org.junit.Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        tableEnv.executeSql(ddl);
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from hudi_4726 " +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20220827020134588') */"
                ), Row.class)
                .print();

        env.execute();
    }
}
