package com.vip.hudi.flink.debug;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class ITTestNodataRollback extends TestUtil {
    // 在批处理场景下，没有数据的话，会出现rollback情况
    String tblName = "ITTestNodataRollback";
    String srcDdl = "create table file_src_tbl (\n" +
            "    id VARCHAR,\n" +
            "    name varchar,\n" +
            "    age int,\n" +
            "    `par` string\n" +
            ") with (\n" +
            "  'connector' = 'filesystem',\n" +
            "  'path' = 'file:///Users/hunter/workspace/hudipr/master-debug/hudi-examples/hudi-examples-debug/src/test/resources/testData2',\n" +
            "  'format' = 'csv'\n" +
            ")";
    String hudiDDL = "CREATE TABLE HUDI_MOR_TBL(\n" +
            "    id STRING PRIMARY KEY NOT ENFORCED,\n" +
            "   `name` STRING,\n" +
            "    age bigint,\n" +
            "    `par` STRING\n" +
            ") PARTITIONED BY (`par`) WITH (\n" +
            "    'connector' = 'hudi',\n" +
            "    'table.type' = 'MERGE_ON_READ',\n" +
            "    'path' = 'file:///Users/hunter/workspace/hudipr/master-debug/hudi-examples/hudi-examples-debug/target/ITTestNodataRollback',\n" +
            "    'compaction.delta_commits' = '2',\n" +
            "    'compaction.schedule.enable' = 'true',\n" +
            "    'compaction.async.enabled' = 'true',\n" +
            "    'changelog.enabled' = 'false',\n" +
            "    'index.type' = 'BUCKET', \n" +
            "    'hoodie.bucket.index.num.buckets'='1', \n" +
            "    'write.tasks' = '1')";
    String inserDml = "insert into HUDI_MOR_TBL " +
            " select " +
            "id," +
            "name," +
            "age," +
            "par" +
            " from file_src_tbl";

    @Test
    public void testWriteBatch() throws ExecutionException, InterruptedException {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(srcDdl);
        tableEnv.executeSql(hudiDDL);
        tableEnv.executeSql(inserDml).await();
        for (int i = 0; i < 10; i++) {
            tableEnv.executeSql(inserDml + " where 1 < 0").await();
        }
    }
}
