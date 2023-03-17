package com.flink.multi.write;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class ITTestPartialUpdate {
    public static StreamTableEnvironment tableEnv;

    public static String PATH = "file:///Users/hunter/workspace/vip/vip-hudi/hudi-debug/target/ITTestPartialUpdate";

    @BeforeEach
    public void before() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.dynamic-table-options.enabled", "true");
    }


    @Test
    public void testFlinkMultiStreamJoin_job1() throws ExecutionException, InterruptedException {
        String src_1 = "create table socket (\n" +
                "id STRING,\n" +
                "name STRING,\n" +
                "ts STRING,\n" +
                "`par` STRING) with (\n" +
                " 'connector' = 'socket'  ," +
                "'hostname'='localhost'\n" +
                ",'port'='9801'\n" +
                ",'format'='csv'\n" +
                " \n" +
                ")";
        String sink_1 = "create table sink_1(\n" +
                "  id STRING,\n" +
                "  name STRING,\n" +
                "  age int,\n" +
                "  ts STRING,\n" +
                "  par string,\n" +
                "  PRIMARY KEY(id) NOT ENFORCED)\n" +
                " PARTITIONED BY (par)\n" +
                " with (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = '" + PATH + "',\n" +
                "  'table.type' = 'MERGE_ON_READ',\n" +
                "  'write.bucket_assign.tasks' = '6',\n" +
                "  'write.tasks' = '3',\n" +
                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
                "  'changelog.enabled' = 'true',\n" +
                "  'index.type' = 'BUCKET',\n" +
                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
                "  'write.precombine.field' = 'ts',\n" +
                "  'write.payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',\n" +
                "  'hoodie.write.log.suffix' = 'job1',\n" +
                "  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider',\n" +
                "  'hoodie.cleaner.policy.failed.writes' = 'NEVER',\n" +
                "  'hoodie.cleaner.policy' = 'KEEP_LATEST_BY_HOURS',\n" +
                "  'hoodie.write.lock.early.conflict.detection.enable' = 'true',\n" +
                "  'hoodie.write.lock.early.conflict.detection.strategy' = 'org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy',\n" +
                "  'hoodie.keep.min.commits' = '1440',\n" +
                "  'hoodie.keep.max.commits' = '2880',\n" +
                "  'compaction.schedule.enabled'='true',\n" +
                "  'compaction.async.enabled'='true',\n" +
                "  'compaction.trigger.strategy'='num_or_time',\n" +
                "  'compaction.delta_commits' ='1',\n" +
                "  'compaction.delta_seconds' ='180',\n" +
                "  'compaction.max_memory' = '3096',\n" +
                "  'clean.async.enabled' ='false',\n" +
                "  'hoodie.metrics.on' = 'false'\n" +
                ")";
        String insert = "insert into sink_1(id, name, ts, par) select id, name, ts, par from socket";
        System.out.println(src_1);
        System.out.println(sink_1);
        System.out.println(insert);

        tableEnv.executeSql(src_1);
        tableEnv.executeSql(sink_1);
        tableEnv.executeSql(insert).await();
    }


    @Test
    public void testFlinkMultiStreamJoin_job2() throws ExecutionException, InterruptedException {
        String src_2 = "create table socket (\n" +
                "id STRING,\n" +
                "age int,\n" +
                "ts STRING,\n" +
                "`par` STRING) with (\n" +
                "'connector' = 'socket'  ," +
                "'hostname'='localhost'\n" +
                ",'port'='9802'\n" +
                ",'format'='csv'\n" +
                " \n" +
                ")";
        String sink_2 = "create table sink_2(\n" +
                "  id STRING,\n" +
                "  name STRING,\n" +
                "  age int,\n" +
                "  ts STRING,\n" +
                "  par string,\n" +
                "  PRIMARY KEY(id) NOT ENFORCED)\n" +
                " PARTITIONED BY (par)\n" +
                " with (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = '" + PATH + "',\n" +
                "  'table.type' = 'MERGE_ON_READ',\n" +
                "  'write.bucket_assign.tasks' = '6',\n" +
                "  'write.tasks' = '3',\n" +
                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
                "  'changelog.enabled' = 'true',\n" +
                "  'index.type' = 'BUCKET',\n" +
                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
                "  'write.precombine.field' = 'ts',\n" +
                "  'write.payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',\n" +
                "  'hoodie.write.log.suffix' = 'job2',\n" +
                "  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider',\n" +
                "  'hoodie.cleaner.policy.failed.writes' = 'NEVER',\n" +
                "  'hoodie.cleaner.policy' = 'KEEP_LATEST_BY_HOURS',\n" +
                "  'hoodie.write.lock.early.conflict.detection.enable' = 'true',\n" +
                "  'hoodie.write.lock.early.conflict.detection.strategy' = 'org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy',\n" +
                "  'hoodie.keep.min.commits' = '1440',\n" +
                "  'hoodie.keep.max.commits' = '2880',\n" +
                "  'compaction.schedule.enabled'='false',\n" +
                "  'compaction.async.enabled'='false',\n" +
                "  'compaction.trigger.strategy'='num_or_time',\n" +
                "  'compaction.delta_commits' ='1',\n" +
                "  'compaction.delta_seconds' ='180',\n" +
                "  'compaction.max_memory' = '3096',\n" +
                "  'clean.async.enabled' ='false',\n" +
                "  'hoodie.metrics.on' = 'false'\n" +
                ")";
        String insert = "insert into sink_2(id, age, ts, par) select id, age, ts, par from socket";
        tableEnv.executeSql(src_2);
        tableEnv.executeSql(sink_2);
        tableEnv.executeSql(insert).await();
    }
    //job1 : id,name,1,par1
    //job2 :  id,1,2,par1

    //job1 : id2,name,1,par1
    //job2 :   id2,2,2,par1

    //job1 : id3,name,1,par1
    //job2 :   id3,4,3,par1
    //job2 :   id3,6,6,par1


    @Test
    public void testRead() throws Exception {
        String sink_1 = "create table sink_1(\n" +
                "  id STRING,\n" +
                "  name STRING,\n" +
                "  age int,\n" +
                "  ts STRING,\n" +
                "  par string,\n" +
                "  PRIMARY KEY(id) NOT ENFORCED)\n" +
                " PARTITIONED BY (par)\n" +
                " with (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path' = '" + PATH + "',\n" +
                "  'table.type' = 'MERGE_ON_READ',\n" +
                "  'write.bucket_assign.tasks' = '6',\n" +
                "  'write.tasks' = '3',\n" +
                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
                "  'changelog.enabled' = 'true',\n" +
                "  'index.type' = 'BUCKET',\n" +
                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
                "  'write.precombine.field' = 'ts',\n" +
                "  'write.payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',\n" +
                "  'hoodie.write.log.suffix' = 'job1',\n" +
                "  'hoodie.write.concurrency.mode' = 'optimistic_concurrency_control',\n" +
                "  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider',\n" +
                "  'hoodie.cleaner.policy.failed.writes' = 'LAZY',\n" +
                "  'hoodie.cleaner.policy' = 'KEEP_LATEST_BY_HOURS',\n" +
                "  'hoodie.write.lock.early.conflict.detection.enable' = 'true',\n" +
                "  'hoodie.write.lock.early.conflict.detection.strategy' = 'org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy',\n" +
                "  'hoodie.keep.min.commits' = '1440',\n" +
                "  'hoodie.keep.max.commits' = '2880',\n" +
                "  'compaction.schedule.enabled'='false',\n" +
                "  'compaction.async.enabled'='false',\n" +
                "  'compaction.trigger.strategy'='num_or_time',\n" +
                "  'compaction.delta_commits' ='1',\n" +
                "  'compaction.delta_seconds' ='180',\n" +
                "  'compaction.max_memory' = '3096',\n" +
                "  'clean.async.enabled' ='false',\n" +
                "  'hoodie.metrics.on' = 'false'\n" +
                ")";
        tableEnv.executeSql(sink_1);
        tableEnv.sqlQuery("select * from sink_1" +
                "/*+ OPTIONS(" +
                "'read.streaming.enabled' = 'false'," +
                "'read.tasks'='1'," +
                "'read.start-commit'='0'," +
                "'read.end-commit'='20231013101032156') */"
        ).execute().print();
    }
}
