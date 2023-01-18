package com.vip.hudi.flink.dingding.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class Zhouzhangyu_bucket_dul extends TestUtil  {
    public String ddl = "CREATE TABLE IF NOT EXISTS ODS_BPM_LBPM_AUDIT_NOTE\n" +
            "(\n" +
            "    `FD_ID` VARCHAR ," +
            "    `partition` VARCHAR,\n" +
            "    PRIMARY KEY (`FD_ID`) NOT ENFORCED\n" +
            "    )\n" +
            "    PARTITIONED BY (`partition`)\n" +
            "    with (\n" +
            "        'connector' = 'hudi'," +
            "        'path' = 'file:///Users/hunter/workspace/hudipr/master-debug/hudi-examples/hudi-examples-debug/target/ods_bpm_lbpm_audit_note',\n" +
            "        'table.type' = 'MERGE_ON_READ',\n" +
            "        'write.bucket_assign.tasks' = '6',\n" +
            "        'write.tasks' = '4',\n" +
            "        -- changelog\n" +
            "        'changelog.enabled' = 'true',\n" +
            "        'clean.retain_commits' = '720',\n" +
            "        'read.streaming.enabled' = 'true',\n" +
            "        'read.start-commit' = 'earliest',\n" +
            "        -- Bucket Index\n" +
            "        'index.type'= 'BUCKET',\n" +
            "        'hoodie.bucket.index.num.buckets'= '6',\n" +
            "        'write.rate.limit'= '6000',\n" +
            "        --'write.task.max.size'= '4096',\n" +
            "        --'write.merge.max_memory'= '2048',\n" +
            "        'compaction.max_memory' = '1024',\n" +
            "        'compaction.tasks' = '6',\n" +
            "        -- 'compaction.async.enabled' = 'true',\n" +
            "        'compaction.trigger.strategy' = 'num_commits',\n" +
            "        'compaction.delta_commits' = '3'\n" +
            "        )\n";


    @Test
    public void testWrite() throws ExecutionException, InterruptedException {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.executeSql(ddl);
        tableEnv.executeSql("CREATE table bulk_insert_src(\n" +
                "    `FD_ID` VARCHAR ," +
                "    `partition` VARCHAR\n" +
                ") with ('connector' = 'filesystem',\n" +
                "        'path' = '/Users/hunter/workspace/hudipr/master-debug/hudi-examples/hudi-examples-debug/src/test/resources/kafka_source.txt',\n" +
                "        'format' = 'csv'\n" +
                ");");

        tableEnv.executeSql("insert into ODS_BPM_LBPM_AUDIT_NOTE select * from bulk_insert_src").await();

    }

    @org.junit.Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        tableEnv.executeSql(ddl);

        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from ODS_BPM_LBPM_AUDIT_NOTE " +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20220829171204521') */"
                ), Row.class)
                .print();
        env.execute();
    }
}
