package com.vip.hudi.flink.mor.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static com.vip.hudi.flink.utils.TestSQLManager.PRINT_DDL;
import static com.vip.hudi.flink.utils.TestSQLManager.SRC_DATAGEN_DDL;

public class ITTestTimestamp extends TestUtil {
    public static String TABLE_NAME = "ITTestTimestamp";
    public static String SRC_TABLE_NAME = "datagen";

    static {
//        pro.put("metadata.enabled", "true");
    }

    /**
     * 官方例子，必须是.execute().print()，
     * 如果用toRetractStream，必须要cast to string
     *
     * @throws Exception
     */
    @Test
    public void testOfficial() throws Exception {
        EnvironmentSettings envSetting = EnvironmentSettings.inStreamingMode();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(1000L);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(5000L);
        tableEnv = StreamTableEnvironment.create(env, envSetting);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tableEnv.executeSql("CREATE VIEW MyView1 AS SELECT LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_ROW_TIMESTAMP(), NOW(), PROCTIME()");
        tableEnv
                .sqlQuery("select * from MyView1")
                .execute()
                .print();
        // 想要转字符串，必须使用强转才可以，不然时区不会生效
        tableEnv.toRetractStream(tableEnv
                        .sqlQuery("select CAST(CURRENT_TIMESTAMP AS STRING) from MyView1"), Row.class)
                .print();
        env.execute();
    }

    @Test
    public void testPrint() throws Exception {
        SRC_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SRC_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SRC_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SRC_SCHEMA.add(new Tuple2<>("ts", "timestamp_ltz(3)"));
        SRC_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.executeSql(SRC_DATAGEN_DDL(SRC_TABLE_NAME, false, SRC_SCHEMA));
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "string"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        tableEnv.executeSql(PRINT_DDL("print", SINK_SCHEMA));
        tableEnv.executeSql("insert into print select '1', 'name', 1, CAST(NOW() as String), 'par' from " + SRC_TABLE_NAME).await();
    }


    /**
     * 写入是timestamp类型。 读取的时候制定是 ltz或者 cast成ltz 也是可以解决时区问题
     *
     * @throws Exception
     */
    @Test
    public void testWriteHudi() throws Exception {
        SRC_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SRC_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SRC_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SRC_SCHEMA.add(new Tuple2<>("ts", "timestamp"));
        SRC_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        // 默认就是这个，不需要加
//        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        tableEnv.executeSql(SRC_DATAGEN_DDL(SRC_TABLE_NAME, false, SRC_SCHEMA));
        tableEnv.executeSql("insert into " + TABLE_NAME + " select id,name,age,ts,'par' from " + SRC_TABLE_NAME).await();
    }


    @org.junit.Test
    public void testRead() throws Exception {
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "timestamp(3)"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.sqlQuery("select * from " + TABLE_NAME +
                "/*+ OPTIONS(" +
                "'read.streaming.enabled' = 'false'," +
                "'read.tasks'='1'," +
                "'read.start-commit'='0'," +
                "'read.end-commit'='20231013101032156') */"
        ).execute().print();

        // cast(ts as string) 才能正确打印
//        tableEnv.toRetractStream(tableEnv.sqlQuery("select id,name,age,cast(ts as string),par from " + TABLE_NAME +
//                        "/*+ OPTIONS(" +
//                        "'read.streaming.enabled' = 'false'," +
//                        "'read.tasks'='1'," +
//                        "'read.start-commit'='0'," +
//                        "'read.end-commit'='20231013101032156') */"
//                ), Row.class)
//                .print();
//        env.execute();
    }
}
