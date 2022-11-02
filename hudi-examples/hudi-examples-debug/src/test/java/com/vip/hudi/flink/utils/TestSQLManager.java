package com.vip.hudi.flink.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TestSQLManager {

    public static Map<String, String> HUDI_DDL_BASE_PROPERTIES = new HashMap<>();
    public static ArrayList<Tuple2<String, String>> HUDI_DDL_BASE_SCHEMA = new ArrayList<Tuple2<String, String>>();

    static {
        HUDI_DDL_BASE_PROPERTIES.put("index.type", "BUCKET");
        HUDI_DDL_BASE_PROPERTIES.put("hoodie.bucket.index.num.buckets", "1");
        HUDI_DDL_BASE_PROPERTIES.put("compaction.delta_commits", "1");
        HUDI_DDL_BASE_PROPERTIES.put("compaction.schedule.enable", "true");
        HUDI_DDL_BASE_PROPERTIES.put("compaction.async.enabled", "true");
        HUDI_DDL_BASE_PROPERTIES.put("changelog.enabled", "false");
        HUDI_DDL_BASE_PROPERTIES.put("precombine.field", "ts");
        HUDI_DDL_BASE_PROPERTIES.put("write.tasks", "1");
//        HUDI_DDL_BASE_PROPERTIES.put("payload.class", "org.apache.hudi.common.model.PartialUpdateAvroPayload");

        // 第一个是 主键，最后一个是分区
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("id", "STRING"));
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("name", "STRING"));
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("age", "bigint"));
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("ts", "bigint"));
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
    }

//    public static String hudiDDL = "CREATE TABLE HUDI_MOR_TBL(\n" +
//            "    id STRING PRIMARY KEY NOT ENFORCED,\n" +
//            "    name string," +
//            "    age bigint,\n" +
//            "    ts bigint," +
//            "    `par` STRING\n" +
//            ") PARTITIONED BY (`par`) WITH (\n" +
//            "    'connector' = 'hudi',\n" +
//            "    'table.type' = 'MERGE_ON_READ',\n" +
//            "    'path' = 'file:///Users/hunter/workspace/hudipr/master-debug/hudi-examples/hudi-examples-debug/target/ITTestMultiStreamWrite',\n" +
//
//            "    'precombine.field'='ts'," +
//            "    'payload.class'='org.apache.hudi.common.model.PartialUpdateAvroPayload'," +
//            "    'write.tasks' = '1')";

    public static String HUDI_DDL(String tblName, String type, String path, Map<String, String> pro, ArrayList<Tuple2<String, String>> schema) {
        if (pro != null)
            HUDI_DDL_BASE_PROPERTIES.putAll(pro);
        StringBuilder proSB = new StringBuilder();
        StringBuilder schemaSB = new StringBuilder();
        String par = schema.get(schema.size() - 1).f0;
        for (int i = 0; i < schema.size() - 1; i++) {
            Tuple2<String, String> t2 = schema.get(i);
            if (i == 0) {
                schemaSB.append(String.format("%s %s PRIMARY KEY NOT ENFORCED,\n", t2.f0, t2.f1));
            } else {
                schemaSB.append(String.format("%s %s,\n", t2.f0, t2.f1));
            }
        }
        schemaSB.append(String.format("%s %s", schema.get(schema.size() - 1).f0, schema.get(schema.size() - 1).f1));
        HUDI_DDL_BASE_PROPERTIES.forEach((k, v) -> {
            proSB.append(String.format("'%s'='%s',\n", k, v));
        });

        return String.format("CREATE TABLE %s(\n" +
                "%s \n" +
                ") PARTITIONED BY (%s) \n" +
                " WITH (\n" +
                "%s" +
                "'table.type' = '%s',\n" +
                "'path' = '%s',\n" +
                "'connector' = 'hudi'\n" +
                ")", tblName, schemaSB, par, proSB, type, path);
    }

    public static String HUDI_DDL_WITH_PRO(String tblName, String type, String path, Map<String, String> pro) {
        return HUDI_DDL(tblName, type, path, pro, HUDI_DDL_BASE_SCHEMA);
    }

    public static String HUDI_DDL_MOR(String tblName, String path) {
        return HUDI_DDL_MOR_WITH_PRO(tblName, path, null);
    }

    public static String HUDI_DDL_MOR_WITH_PRO(String tblName, String path, Map<String, String> pro) {
        return HUDI_DDL_WITH_PRO(tblName, "MERGE_ON_READ", path, pro);
    }


    public static String HUDI_DDL_MOR_WITH_PRO_SCHEMA(String tblName, String path, Map<String, String> pro, ArrayList<Tuple2<String, String>> schema) {
        return HUDI_DDL(tblName, "MERGE_ON_READ", path, pro, schema);
    }

    public static void main(String[] args) {
        System.out.println(HUDI_DDL_MOR("tt", "ss"));
    }
}
