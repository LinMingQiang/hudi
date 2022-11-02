package com.vip.hudi.flink.debug;


import com.vip.hudi.flink.utils.TestSQLManager;
import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * 多流拼接
 * 1：如果两次写入的 schema不一样，在查询的时候是使用最近写入的schema，这样导致查出来的结果不对
 * 2：修改的方式可以在查询的时候选择schema的时候从Properties文件里面拿
 */
public class ITTestPartialPayload extends TestUtil {
    public static String HUDI_DDL;
    public static Map<String, String> pro;
    public static ArrayList<Tuple2<String, String>> schema;
    public static String TABLE_NAME = "ITTestPartialPayload";
    static {
        pro = new HashMap<>();
        pro.put("metadata.enabled", "true");
        pro.put("payload.class", "org.apache.hudi.common.model.PartialUpdateAvroPayload");
        schema = new ArrayList<>();
        // 第一个是 主键，最后一个是分区
        schema.add(new Tuple2<>("id", "STRING"));
        schema.add(new Tuple2<>("name", "STRING"));
        schema.add(new Tuple2<>("age", "bigint"));
        schema.add(new Tuple2<>("ts", "bigint"));
        schema.add(new Tuple2<>("`par`", "STRING"));
        HUDI_DDL = TestSQLManager.HUDI_DDL_MOR_WITH_PRO_SCHEMA(
                TABLE_NAME,
                FILE_HUDI_PATH+ TABLE_NAME,
                pro,
                schema);
        init(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
    }

    /**
     * 1,name,1,1,par1
     * 1,,2,2,par1
     * -- 1,name,2,2,par1
     *
     * @throws Exception
     */
    @Test
    public void testWriteA() throws Exception {
        tableEnv.executeSql(HUDI_DDL);
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, age, ts, par)  values('1',2,2,'par1')").await();
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, name, ts, par) values('1','name',1,'par1')").await();
    }


    /**
     * 去掉一个字段age
     *
     * @throws Exception
     */
    @Test
    public void testWriteB() throws Exception {
        schema.clear();
        schema.add(new Tuple2<>("id", "STRING"));
        schema.add(new Tuple2<>("name", "STRING"));
        schema.add(new Tuple2<>("ts", "bigint"));
        schema.add(new Tuple2<>("`par`", "STRING"));
        HUDI_DDL = TestSQLManager.HUDI_DDL_MOR_WITH_PRO_SCHEMA(
                TABLE_NAME,
                FILE_HUDI_PATH +TABLE_NAME,
                pro,
                schema);
        tableEnv.executeSql(HUDI_DDL);
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, name, ts, par) values('1','name',1,'par1')").await();

    }

    @Test
    public void testWriteC() throws Exception {
        schema.clear();
        schema.add(new Tuple2<>("id", "STRING"));
        schema.add(new Tuple2<>("age", "bigint"));
        schema.add(new Tuple2<>("ts", "bigint"));
        schema.add(new Tuple2<>("`par`", "STRING"));
        HUDI_DDL = TestSQLManager.HUDI_DDL_MOR_WITH_PRO_SCHEMA(
                TABLE_NAME,
                FILE_HUDI_PATH +TABLE_NAME,
                pro,
                schema);
        tableEnv.executeSql(HUDI_DDL);
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, age, ts, par)  values('1',3,3,'par1')").await();

    }

    @org.junit.Test
    public void testRead() throws Exception {
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from  " + TABLE_NAME +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20231013101032156') */"
                ), Row.class)
                .print();
        env.execute();
    }
}
