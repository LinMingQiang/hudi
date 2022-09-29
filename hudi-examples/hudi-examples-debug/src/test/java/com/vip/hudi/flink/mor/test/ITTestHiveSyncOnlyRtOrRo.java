package com.vip.hudi.flink.mor.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class ITTestHiveSyncOnlyRtOrRo {

    public static String path = "file:///Users/hunter/workspace/hudipr/master-debug/hudi-examples/hudi-examples-debug/target/ITTestHiveSyncOnlyRtOrRo";
    public static StreamTableEnvironment tableEnv = null;
    public static StreamExecutionEnvironment env = null;

    public static String ddl = "CREATE TABLE ITTestHiveSyncOnlyRtOrRo(\n" +
            "                                     id string,\n" +
            "                                     msg string,\n" +
            "                                     `partition` STRING,\n" +
            "                                     PRIMARY KEY(id) NOT ENFORCED\n" +
            ") PARTITIONED BY (`partition`) WITH (\n" +
            "    'connector' = 'hudi',\n" +
            "    'table.type' = 'MERGE_ON_READ',\n" +
            "    'path' = '"+path+"',\n" +
            "    'write.payload.class' = 'org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload',\n" +
            "    'compaction.delta_commits' = '3',\n" +
            "    'read.streaming.enabled' = 'true',\n" +
            "    'read.streaming.check-interval' = '1',\n" +
            "    'read.start-commit' = '0',\n" +
            "    'read.end-commit' = '20230223203340880',\n" +
            "    'compaction.schedule.enable' = 'true',\n" +
            "    'compaction.async.enabled' = 'true',\n" +
            "    'changelog.enabled' = 'false',\n" +
            "    'index.type' = 'BUCKET', \n" +
            "    'hoodie.bucket.index.num.buckets'='3', \n" +
            "    'hive_sync.enable' = 'true',\n" +
            "    'hive_sync.mode' = 'hms',\n" +
            "    'hive_sync.metastore.uris' = 'thrift://localhost:9083',\n" + //bigdata-hiveserver.vip.vip.com
            "    'hive_sync.db' = 'test',\n" +
            "    'hive_sync.table' = 'ITTestHiveSyncOnlyRtOrRo',"+
            "    'hive_sync.table.ro_or_rt'='both'," +
            "    'write.tasks' = '1')\n";

    static{
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(5000L);
    }
    public static void init(RuntimeExecutionMode mode){
        env.setRuntimeMode(mode);
        tableEnv = StreamTableEnvironment.create(env);
    }

    @Test
    public void testtWrite() throws Exception {
        init(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        tableEnv.executeSql(ddl);
        System.out.println(ddl);
        tableEnv.executeSql("INSERT INTO ITTestHiveSyncOnlyRtOrRo values ('id1','t1','par1')").await();
        tableEnv.executeSql("INSERT INTO ITTestHiveSyncOnlyRtOrRo values ('id1','t2','par1')").await();
        tableEnv.executeSql("INSERT INTO ITTestHiveSyncOnlyRtOrRo values ('id1','t3','par1')").await();
        tableEnv.executeSql("INSERT INTO ITTestHiveSyncOnlyRtOrRo values ('id1','t4','par1')").await();
        tableEnv.executeSql("INSERT INTO ITTestHiveSyncOnlyRtOrRo values ('id1','t5','par1')").await();

    }

    @org.junit.Test
    public void testRead() throws Exception {
        init(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        tableEnv.executeSql(ddl);
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from ITTestHiveSyncOnlyRtOrRo " +
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
