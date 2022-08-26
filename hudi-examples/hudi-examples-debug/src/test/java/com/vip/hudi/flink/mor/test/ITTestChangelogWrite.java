package com.vip.hudi.flink.mor.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.assertEquals;

/**
 * 主要测试changelog的场景，例如flink 聚合结果输出，mysql的cdc数据等.
 */
public class ITTestChangelogWrite extends TestUtil{
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
        init(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(FILE_SRC_HUDI_TBL(pathName));

        Row ins = Row.ofKind(RowKind.UPDATE_AFTER, StringData.fromString("id1"),
                StringData.fromString("Danny"), 24,
                TimestampData.fromEpochMillis(1),
                StringData.fromString("par1"));
//        BinaryRowData ins = insertRow((RowType)ROW_DATA_TYPE.getLogicalType(),
//                StringData.fromString("id1"),
//                StringData.fromString("Danny"), 24,
//                TimestampData.fromEpochMillis(1),
//                StringData.fromString("par1"));
//        BinaryRowData update = insertRow(
//                (RowType)ROW_DATA_TYPE.getLogicalType(),
//                StringData.fromString("id1"),
//                StringData.fromString("Danny"),
//                24,
//                TimestampData.fromEpochMillis(1),
//                StringData.fromString("par1"));
//        update.setRowKind(RowKind.UPDATE_AFTER);

        ArrayList<Row> r = new ArrayList<>();
        r.add(ins);
//        r.add(update);

        tableEnv.fromDataStream(env.fromCollection(r)).printSchema();
        tableEnv.toRetractStream(tableEnv.fromDataStream(env.fromCollection(r)), Row.class).print();

        env.execute() ;
//
//        String insertSql = "insert into file_src_hudi_tbl /*+ OPTIONS(" +
//                "    'changelog.enabled' = 'true'," +
//                "    'hoodie.table.version.fields' = 'dt,hm', \n" +
//                "    'hoodie.table.version.values' = '2022-01-01,1100'\n" +
//                ") */" +
//                " select " +
//                "id," +
//                "name," +
//                "count(1) as age," +
//                "DATE_FORMAT('2011-11-11 11:11:11','yyyy-MM-dd HH:mm:ss') as rowtime," +
//                "DATE_FORMAT('2011-11-11 11:11:11','yyyy-MM-dd') as dt" +
//                " from file_src_tbl group by id, name";

//        tableEnv.executeSql(insertSql).await();
    }
    @org.junit.Test
    public void testRead() throws Exception {
        init(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(FILE_SRC_HUDI_TBL(pathName));
        tableEnv.toRetractStream(tableEnv.sqlQuery("select count(1) from file_src_hudi_tbl " +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'write.precombine' = 'true',"+
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20240621170750480') */"), Row.class)
                .print();
        env.execute();
    }

    public static BinaryRowData insertRow(RowType rowType, Object... fields) {
        LogicalType[] types = rowType.getFields().stream().map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
        assertEquals(
                "Filed count inconsistent with type information",
                fields.length,
                types.length);
        BinaryRowData row = new BinaryRowData(fields.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.reset();
        for (int i = 0; i < fields.length; i++) {
            Object field = fields[i];
            if (field == null) {
                writer.setNullAt(i);
            } else {
                BinaryWriter.write(writer, i, field, types[i], InternalSerializers.create(types[i]));
            }
        }
        writer.complete();
        return row;
    }
}
