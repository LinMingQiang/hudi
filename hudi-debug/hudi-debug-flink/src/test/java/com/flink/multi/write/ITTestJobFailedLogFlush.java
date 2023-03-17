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
 *
 *
 * - .hoodie
 *  t1.deltacommit
 *  t2.deltacommit.inflight
 *  -file
 *    par1: f1.log
 *    par2: f2.log (写入成功，commit失败)
 * 非并发下------------------------------------------------------------------
 * 读取问题  ：部分log写入完成，job失败了，log的数据会不会被读取。
 * 验证方法：先成功一次，然后打断点在文件生成后停止，deltacommit是失败的，
 * id2,2,2,par1
 * id3,4,3,par1
 * id4,5,6,par1
 * 读取的时候数据正常失败的commit虽然文件在但是并不会被读取，但是脏数据不会被手动删掉。
 *        -- 看看IncrementalInputSplits.getInputSplits, 似乎一个partition下的会被读取，但是会被最后一个commit的instant过滤数据。
 *
 * 这时候，再执行一次，未完成的会被rollback，新的deltacommit成功后，因为失败的那个文件不会被删除，这时候再读取：
 * 答：虽然读取的时候会加载文件，但是会过滤里面的每个block，看看当前block的instant在不在成功的commit。如果不是就跳过。
 *
 *
 * ------------------------------------------------------------------
 * compaction 问题：
 * 非并发 compaction 会不会把失败的文件也compaction进去：
 * 关闭compaction，然后先成功一次，打断点，在文件生成后停止，deltacommit是失败的；
 * 然后重启程序，在compaction地方打断点（需要改代码，让他没数据也可以执行调度）.
 *
 * 答 ： 我估计，失败的文件会被调度进去，但是会过滤他里面的数据，只有有成功的commit的block才会被压缩。
 *
 *
 * 并发下 ------------------------------------------------------------------
 *
 * - .hoodie
 *  t1.deltacommit
 *  t2.deltacommit.inflight
 *  -file
 *    par1: f1.log
 *    par2: f2.log (部分成功，还有几个task的文件还没提交，还未提交)
 *  -compaction
 *      - t3
 *      之后的数据都是往t3的log文件写入了。
 *
 * 问题1 ：这时候出发compaction，由于 f2.log正常也是属于 compaction的调度范围，这时候会调度进去吗？
 * 1： 如果调度进去，那结果是不对的，丢掉了几个还在写入的？
 * 2： 不调度进去，那下一次是怎么识别这些 文件的，按正常来说，compaction被调度后生成新的instant，之后的数据写入都是基于这个instant
 *      重新建立文件的，过去的数据被
 * 答：会被调度进去，但是压缩的时候会过滤里面的block，如果这个block没有成功的commit对应就不被采用。但是当compaction完成后，t2也刚好才完成，
 * 这时候因为t2没有被compaction进去，下一次的compaction不会包含t2，因为他要拉 t1.parquet + t1.log 的数据来compaction。
 *
 *
 * 问题2：由于过一段时间，t2就成功了，那正常t2文件应该是可用的，但是因为compaction已经调度过了，也成功了，这个时候t2是不是永远也无法读取到了.
 *
 * 验证：
 *      1：成功一次，然后断点在compaction schedule， 启动任务2，log写入成功卡主不动。
 *      2：切到compaction，查看调度计划里面是否包含了 log2 文件
 *          -- 1：包含了，真正压缩的时候会不会把未完成的数据包含进去
 *          -- 2：不包含，那当t2完成了，下一次调度，会不会包含呢。
 *
 */
public class ITTestJobFailedLogFlush {
    public static StreamTableEnvironment tableEnv;

    public static String PATH = "file:///Users/hunter/workspace/vip/vip-hudi/hudi-debug/target/ITTestJobFailedLogFlush";
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
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.dynamic-table-options.enabled", "true");
    }

    /** job2 :
     * id2,2,2,par1
     * id3,4,3,par1
     * id4,5,6,par1
     * 正常情况下是什么样子的
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testNormal() throws ExecutionException, InterruptedException {
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
                "  'compaction.schedule.enabled'='false',\n" +
                "  'compaction.async.enabled'='false',\n" +
                "  'compaction.delta_commits' ='1'\n" +
                ")";
        String insert1 = "insert into sink_1 select id, name, 1 as age, ts, par from socket1";
        tableEnv.executeSql(src_1);
        tableEnv.executeSql(sink_1);
        StatementSet set = tableEnv.createStatementSet();
        set.addInsertSql(insert1);
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
//
//    @Test
//    public void testOneJob() throws ExecutionException, InterruptedException {
//        String sink_1 = "create table sink_1(\n" +
//                "  id STRING,\n" +
//                "  name STRING,\n" +
//                "  age int,\n" +
//                "  ts STRING,\n" +
//                "  par string,\n" +
//                "  PRIMARY KEY(id) NOT ENFORCED)\n" +
//                " PARTITIONED BY (par)\n" +
//                " with (\n" +
//                "  'connector' = 'hudi',\n" +
//                "  'path' = '" + PATH + "',\n" +
//                "  'table.type' = 'MERGE_ON_READ',\n" +
//                "  'write.bucket_assign.tasks' = '6',\n" +
//                "  'write.tasks' = '3',\n" +
//                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
//                "  'changelog.enabled' = 'true',\n" +
//                "  'index.type' = 'BUCKET',\n" +
//                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
//                "  'write.precombine.field' = 'ts',\n" +
//                "  'write.payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',\n" +
//                "  'hoodie.write.log.suffix' = 'job1',\n" +
//                "  'hoodie.write.concurrency.mode' = 'optimistic_concurrency_control',\n" +
//                "  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider',\n" +
//                "  'hoodie.cleaner.policy.failed.writes' = 'LAZY',\n" +
//                "  'hoodie.cleaner.policy' = 'KEEP_LATEST_BY_HOURS',\n" +
//                "  'hoodie.write.lock.early.conflict.detection.enable' = 'true',\n" +
//                "  'hoodie.write.lock.early.conflict.detection.strategy' = 'org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy',\n" +
//                "  'compaction.schedule.enabled'='false',\n" +
//                "  'compaction.async.enabled'='false',\n" +
//                "  'compaction.delta_commits' ='1',\n" +
//                "  'hoodie.metrics.on' = 'false'\n" +
//                ")";
//        String sink_2 = "create table sink_2(\n" +
//                "  id STRING,\n" +
//                "  name STRING,\n" +
//                "  age int,\n" +
//                "  ts STRING,\n" +
//                "  par string,\n" +
//                "  PRIMARY KEY(id) NOT ENFORCED)\n" +
//                " PARTITIONED BY (par)\n" +
//                " with (\n" +
//                "  'connector' = 'hudi',\n" +
//                "  'path' = '" + PATH + "',\n" +
//                "  'table.type' = 'MERGE_ON_READ',\n" +
//                "  'write.bucket_assign.tasks' = '6',\n" +
//                "  'write.tasks' = '3',\n" +
//                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
//                "  'changelog.enabled' = 'true',\n" +
//                "  'index.type' = 'BUCKET',\n" +
//                "  'hoodie.bucket.index.num.buckets' = '5',\n" +
//                "  'write.precombine.field' = 'ts',\n" +
//                "  'write.payload.class' = 'org.apache.hudi.common.model.PartialUpdateAvroPayload',\n" +
//                "  'hoodie.write.log.suffix' = 'job2',\n" +
//                "  'hoodie.write.concurrency.mode' = 'optimistic_concurrency_control',\n" +
//                "  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider',\n" +
//                "  'hoodie.cleaner.policy.failed.writes' = 'LAZY',\n" +
//                "  'hoodie.cleaner.policy' = 'KEEP_LATEST_BY_HOURS',\n" +
//                "  'hoodie.write.lock.early.conflict.detection.enable' = 'true',\n" +
//                "  'hoodie.write.lock.early.conflict.detection.strategy' = 'org.apache.hudi.table.marker.SimpleTransactionDirectMarkerBasedEarlyConflictDetectionStrategy',\n" +
//                "  'compaction.schedule.enabled'='true',\n" +
//                "  'hoodie.write.lock.client.wait_time_ms_between_retry'='10000', " +
//                "  'compaction.async.enabled'='true',\n" +
//                "  'compaction.delta_commits' ='1'\n" +
//                ")";
//        String insert2 = "insert into sink_2(id, age, ts, par) select id, age, ts, par from socket2";
//        String insert1 = "insert into sink_1(id, name, ts, par) select id, name, ts, par from socket1";
//
//        tableEnv.executeSql(src_1);
//        tableEnv.executeSql(sink_1);
//        tableEnv.executeSql(src_2);
//        tableEnv.executeSql(sink_2);
//        StatementSet set = tableEnv.createStatementSet();
//        set.addInsertSql(insert2);
//        set.addInsertSql(insert1);
//        set.execute().await();
//    }

// id2,2,2,par1


    //job1 : id,name,1,par1
    //job2 : id,1,2,par1

    //job1 : id2,name,1,par1
    //job2 :   id2,2,2,par1

    //job1 : id3,name,1,par1
    //job2 :   id3,4,3,par1
    //job2 :   id3,6,6,par1


}
