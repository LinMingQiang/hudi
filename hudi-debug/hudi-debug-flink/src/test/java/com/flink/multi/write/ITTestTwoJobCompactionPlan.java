package com.flink.multi.write;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

/**
 * 问题1：当在生成plan的时候，其他的write是否可以继续写数据到log文件
 * 例如
 * -/数据：
 * f1_instant1.log_job1   --- a
 * f1_instant1.log_job2   --- b
 *
 * -/.hoodie
 * instant1.commit
 * j1_t1.deltacommit
 * j2_t2.deltacommit.inflight
 *
 * instant2.compaction.request
 * 这时候如果触发了compaction。他的计划，会不会把 a和b都压缩呢，如果都压缩，那就会丢数据，因为 b 还在写入。如果只做a, 那b的数据什么时候压缩？
 * 1：因为compaction的时候是拿
 *
 * 问题2：由于过一段时间，t2就成功了，那正常t2文件应该是可用的，但是因为compaction已经调度过了，也成功了，这个时候t2是不是永远也无法读取到了.
 *
 * 验证：
 *  1：job1成功写入一个deltacommit : t1.deltacommit (id1,1,2,par1)
 *  2：job2成功写入一个数据，但是还没commit，断点卡主 : t2.deltacommit.inflight (id2,1,2,par1)
 *  3：job1出发compaction schedule，这时候生成compaction.request。
 *  4：
 *      4.1 等compaction压缩完成，这时候查询parquet，看看job2的数据是否有，如果没有，说明compaction的时候没有压缩未完成的deltacommit
 *          --答：compaction 完成后，没有t2的数据，
 *          （有个问题，如果开了clean，这时候触发了clean，但是t2没完成，但是t2的log会被删除掉？？）
 *            -- 这时候完成job2，这时候就是.hoodie
 *              t1.deltacommit t2.deltacommit t3.commit （没有t2的数据）
 *              这时候再触发一次 compaction，这时候查看是否 包含t2的数据（正常应该不包含了，所以这里是有问题的）
 *            --答：schedule的时候 这个log文件会被加载出来，会在plan里面，
 *              1：如果在compaction的时候因为文件没有被close，所以会报错的，数据就没被合进去。
 *              2：如果文件是正常close的，但是t2还没commit，这时候数据是会被过滤掉也不会被合进去。
 *      4.2 生成request后 在compaction.execute卡主，t2完成，然后正常compaction，这时候应该跟正常的流程是一样的，没问题
 *           答： 参考4.1的
 *      4.3 生成request后 卡主，给t2发数据，这时候t2写的log文件是 t3的。然后正常完成compaction，这时候是不是t2的log文件没有在压缩里面。数据是否能正常
 *          答：是可以的，t2生成的log会是 compaction.request的instant，虽然不会被压缩到当前的compaction，但是在下一次的compaction会被合进去。
 *      4.4 先给t2正常生成log文件一次，但是compaction 为 2commit执行一次，t2第二次写入的是卡主没提交，这时候t2的第一次的log数据会不会被压缩进去呢。
 *          （我估计是可以的，参考1）
 */
public class ITTestTwoJobCompactionPlan {
    public static StreamTableEnvironment tableEnv;
    public static String TBL_NAME = "ITTestTwoJobCompactionPlan";
    public static String PATH = "file://" + System.getProperty("user.dir") + "/target/" + TBL_NAME;
    String readDDL = "create table sink_1(\n" +
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
            "  'hoodie.bucket.index.num.buckets' = '2',\n" +
            "  'changelog.enabled' = 'true',\n" +
            "  'index.type' = 'BUCKET',\n" +
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
            "  'compaction.delta_commits' ='3',\n" +
            "  'compaction.delta_seconds' ='180',\n" +
            "  'compaction.max_memory' = '3096',\n" +
            "  'clean.async.enabled' ='false',\n" +
            "  'clean.retain_commits' ='5'," +
            "  'hoodie.metrics.on' = 'false'\n" +
            ")";

    String src_2 = "create table socket2 (\n" +
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


    String src_1 = "create table socket1 (\n" +
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

    @BeforeEach
    public void before() {
        // 多流拼接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.dynamic-table-options.enabled", "true");
    }


    @Test
    public void testOneJob() throws ExecutionException, InterruptedException {
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
                "  'hoodie.bucket.index.num.buckets' = '2',\n" +
                "  'changelog.enabled' = 'true',\n" +
                "  'index.type' = 'BUCKET',\n" +
                "  'write.precombine.field' = 'ts',\n" +
                "  'write.payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',\n" +
                "  'hoodie.write.log.suffix' = 'job1',\n" +
                "  'hoodie.write.concurrency.mode' = 'optimistic_concurrency_control',\n" +
                "  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider',\n" +
                "  'hoodie.cleaner.policy.failed.writes' = 'LAZY',\n" +
                "  'hoodie.cleaner.policy' = 'KEEP_LATEST_BY_HOURS',\n" +
                "  'hoodie.write.lock.early.conflict.detection.enable' = 'true',\n" +
                "  'hoodie.write.lock.early.conflict.detection.strategy' = 'org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy',\n" +
                "  'compaction.schedule.enabled'='true',\n" +
                "  'compaction.async.enabled'='true',\n" +
                "  'compaction.delta_commits' ='3',\n" +
                "  'clean.async.enabled' ='true',\n" +
                "  'clean.retain_commits' ='5'," +
                "  'hoodie.metrics.on' = 'false'\n" +
                ")";

        String insert1 = "insert into sink_1(id, name, ts, par) select id, name, ts, par from socket1";

        tableEnv.executeSql(src_1);
        tableEnv.executeSql(sink_1);
        StatementSet set = tableEnv.createStatementSet();
        set.addInsertSql(insert1);
        set.execute().await();
    }


    //job1 : id,name,1,par1

    //job1 :  id1,1,2,par1
    //job1 :  id1,1,8,par1
    //job2 :  id2,1,3,par1
    //job2 :  id3,1,3,par1
    //job2 :  id4,1,3,par1





    //
    @Test
    public void testOneJob2() throws ExecutionException, InterruptedException {
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
                "  'hoodie.bucket.index.num.buckets' = '2',\n" +
                "  'changelog.enabled' = 'true',\n" +
                "  'index.type' = 'BUCKET',\n" +
                "  'write.precombine.field' = 'ts',\n" +
                "  'write.payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',\n" +
                "  'hoodie.write.log.suffix' = 'job2',\n" +
                "  'hoodie.write.concurrency.mode' = 'optimistic_concurrency_control',\n" +
                "  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider',\n" +
                "  'hoodie.cleaner.policy.failed.writes' = 'LAZY',\n" +
                "  'hoodie.cleaner.policy' = 'KEEP_LATEST_BY_HOURS',\n" +
                "  'hoodie.write.lock.early.conflict.detection.enable' = 'true',\n" +
                "  'hoodie.write.lock.early.conflict.detection.strategy' = 'org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy',\n" +
                "  'compaction.schedule.enabled'='false',\n" +
                "  'compaction.async.enabled'='false',\n" +
                "  'clean.retain_commits' ='5'," +
                "  'clean.async.enabled' ='false',\n" +
                "  'compaction.delta_commits' ='3'\n" +
                ")";
        String insert2 = "insert into sink_2(id, age, ts, par) select id, age, ts, par from socket2";
        tableEnv.executeSql(src_2);
        tableEnv.executeSql(sink_2);
        StatementSet set = tableEnv.createStatementSet();
        set.addInsertSql(insert2);
        set.execute().await();
    }
    @Test
    public void testRead() throws Exception {
        tableEnv.executeSql(readDDL);
        tableEnv.sqlQuery("select * from sink_1" +
                "/*+ OPTIONS(" +
                "'read.streaming.enabled' = 'false'," +
                "'read.tasks'='1'," +
                "'read.start-commit'='0'," +
                "'read.end-commit'='20231013101032156') */"
        ).execute().print();
    }

}
