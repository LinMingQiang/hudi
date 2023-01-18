package com.vip.hudi.flink.mor.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static junit.framework.TestCase.assertEquals;

/**
 * 主要测试changelog的场景，例如flink 聚合结果输出，mysql的cdc数据等.
 */
public class ITTestChangelogWrite extends TestUtil {
    String pathName = "ITTestChangelogWrite";
    public static final DataType ROW_DATA_TYPE = DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.VARCHAR(20)),// record key
                    DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
                    DataTypes.FIELD("age", DataTypes.INT()),
                    DataTypes.FIELD("rowtime", DataTypes.TIMESTAMP(3)), // precombine field
                    DataTypes.FIELD("dt", DataTypes.VARCHAR(10)))
            .notNull();

    @Test
    public void testWrite() throws Exception {
        initEnv(RuntimeExecutionMode.STREAMING);
        tableEnv.getConfig().set("table.exec.sink.upsert-materialize","none");
        tableEnv.executeSql(HUDI_MOR_TBL(pathName));
        Row ins = Row.ofKind(RowKind.UPDATE_AFTER,
                "id1",
                "Danny",
                24,
                "1",
                "par1");
        Row delete = Row.ofKind(RowKind.DELETE, "id1",
                "Danny",
                24,
                "2",
                "par1");
        ArrayList<Row> r = new ArrayList<>();
        r.add(ins);
        r.add(delete);

        Schema shema = Schema.newBuilder()
                .column("id", DataTypes.VARCHAR(20))
                .column("name", DataTypes.VARCHAR(10))
                .column("age", DataTypes.INT())
                .column("rowtime", DataTypes.TIMESTAMP(3))
                .column("dt", DataTypes.VARCHAR(10))
                .build();

        Table tb = tableEnv.fromChangelogStream(env.fromCollection(r), shema);
        tb.printSchema();
        tableEnv.createTemporaryView("file_src_tbl", tb);

//        tableEnv.toRetractStream(tableEnv.fromChangelogStream(env.fromCollection(r)), Row.class).print();
//        env.execute();
//
        String insertSql = "insert into HUDI_MOR_TBL /*+ OPTIONS(" +
                "    'changelog.enabled' = 'true'," +
                "    'hoodie.table.version.fields' = 'dt,hm', \n" +
                "    'hoodie.table.version.values' = '2022-01-01,1100'\n" +
                ") */" +
                "  select * from file_src_tbl";

        tableEnv.executeSql(insertSql).await();
    }

    @org.junit.Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(HUDI_MOR_TBL(pathName));
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from HUDI_MOR_TBL " +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'write.precombine' = 'true'," +
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20240621170750480') */"), Row.class)
                .print();
        env.execute();
    }

}
