package com.vip.hudi.flink;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import static com.vip.hudi.flink.utils.TestSQLManager.*;

public class ITTestSimpleDemo extends TestUtil {
    public static String TABLE_NAME = "ITTestSimpleDemo";

    public static String SRC_TABLE_NAME = "socket";

    static {
        SINK_PRO.put("payload.class", "org.apache.hudi.common.model.PartialUpdateAvroPayload");
        SRC_PRO.put("hostname", "localhost");
        SRC_PRO.put("port", "9877");
        SRC_PRO.put("format", "csv");
    }

    /**
     * 1,name,1,1,par1
     * 1,,2,2,par1
     * -- 1,name,2,2,par1
     * 建表全字段，否则下面的写不进去.
     * @throws Exception
     */
    @Test
    public void testSimpleDemoBatch() throws Exception {
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, age, ts, par)  values('1',2,2,'par1')").await();
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, name, ts, par) values('1','name',1,'par1')").await();
    }


    // 1,name,1,par1
    @Test
    public void testSimpleDemoStream() throws Exception {
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "string"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.STREAMING, TABLE_NAME);
        SRC_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SRC_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SRC_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SRC_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        System.out.println(SRC_SOCKET_DDL(SRC_TABLE_NAME, SRC_SCHEMA, SRC_PRO));
        tableEnv.executeSql(SRC_SOCKET_DDL(SRC_TABLE_NAME, SRC_SCHEMA, SRC_PRO));
        tableEnv.executeSql("insert into "+TABLE_NAME+" select id, name, age, CAST(NOW() as String) as ts, par from " + SRC_TABLE_NAME).await();
    }

    @org.junit.Test
    public void testRead() throws Exception {
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "string"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.sqlQuery("select * from " + TABLE_NAME +
                "/*+ OPTIONS(" +
                "'read.streaming.enabled' = 'false'," +
                "'read.tasks'='1'," +
                "'read.start-commit'='0'," +
                "'read.end-commit'='20231013101032156') */"
        ).execute().print();
    }
}