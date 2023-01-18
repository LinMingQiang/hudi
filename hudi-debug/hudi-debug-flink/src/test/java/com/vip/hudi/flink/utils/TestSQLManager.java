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

        // 第一个是 主键，最后一个是分区
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("id", "STRING"));
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("name", "STRING"));
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("age", "bigint"));
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("ts", "bigint"));
        HUDI_DDL_BASE_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
    }

    public static String HUDI_DDL(String tblName, String type, String path, Map<String, String> pro, ArrayList<Tuple2<String, String>> schema) {
        if (pro != null && !pro.isEmpty())
            HUDI_DDL_BASE_PROPERTIES.putAll(pro);
        StringBuilder proSB = new StringBuilder();
        StringBuilder schemaSB = new StringBuilder();
        if (schema != null && !schema.isEmpty())
            HUDI_DDL_BASE_SCHEMA = schema;
        String par = HUDI_DDL_BASE_SCHEMA.get(HUDI_DDL_BASE_SCHEMA.size() - 1).f0;
        for (int i = 0; i < HUDI_DDL_BASE_SCHEMA.size() - 1; i++) {
            Tuple2<String, String> t2 = HUDI_DDL_BASE_SCHEMA.get(i);
            if (i == 0) {
                schemaSB.append(String.format("%s %s PRIMARY KEY NOT ENFORCED,\n", t2.f0, t2.f1));
            } else {
                schemaSB.append(String.format("%s %s,\n", t2.f0, t2.f1));
            }
        }
        schemaSB.append(String.format("%s %s", HUDI_DDL_BASE_SCHEMA.get(HUDI_DDL_BASE_SCHEMA.size() - 1).f0, HUDI_DDL_BASE_SCHEMA.get(HUDI_DDL_BASE_SCHEMA.size() - 1).f1));
        HUDI_DDL_BASE_PROPERTIES.forEach((k, v) -> {
            proSB.append(String.format("'%s'='%s',\n", k, v));
        });

        return String.format("CREATE TABLE %s(\n" +
                "%s \n" +
                ") PARTITIONED BY (%s) \n" +
                " WITH (\n" +
                "%s" +
                "'table.type' = '%s',\n" +
                "'hoodie.datasource.write.hive_style_partitioning'='true',\n" +
                "'hive_sync.partition_extractor_class'='org.apache.hudi.hive.HiveStylePartitionValueExtractor',\n" +
                "'path' = '%s',\n" +
                "'connector' = 'hudi'\n" +
                ")", tblName, schemaSB, par, proSB, type, path);
    }


    public static String SRC_DATAGEN_DDL(String tblName, boolean isStreaming, ArrayList<Tuple2<String, String>> schema) {
        String pro = isStreaming ? "'rows-per-second'='10'," : "'number-of-rows'='10',";
        StringBuilder schemaSB = new StringBuilder();
        if (schema != null)
            HUDI_DDL_BASE_SCHEMA = schema;
        for (int i = 0; i < HUDI_DDL_BASE_SCHEMA.size(); i++) {
            Tuple2<String, String> t2 = HUDI_DDL_BASE_SCHEMA.get(i);
            if (i < HUDI_DDL_BASE_SCHEMA.size() - 1) {
                schemaSB.append(String.format("%s %s,\n", t2.f0, t2.f1));
            }
        }
        schemaSB.append(String.format("%s %s", HUDI_DDL_BASE_SCHEMA.get(HUDI_DDL_BASE_SCHEMA.size() - 1).f0, HUDI_DDL_BASE_SCHEMA.get(HUDI_DDL_BASE_SCHEMA.size() - 1).f1));

        return String.format("create table %s (\n" +
                "%s" +
                ") with (\n" +
                "  %s \n" +
                "  'connector' = 'datagen'" +
                ")", tblName, schemaSB, pro);
    }

    public static String SRC_SOCKET_DDL(String tblName, ArrayList<Tuple2<String, String>> schema, Map<String, String> pro) {
        StringBuilder schemaSB = new StringBuilder();
        StringBuilder proSB = new StringBuilder();
        if (schema != null)
            HUDI_DDL_BASE_SCHEMA = schema;
        for (int i = 0; i < HUDI_DDL_BASE_SCHEMA.size(); i++) {
            Tuple2<String, String> t2 = HUDI_DDL_BASE_SCHEMA.get(i);
            if (i < HUDI_DDL_BASE_SCHEMA.size() - 1) {
                schemaSB.append(String.format("%s %s,\n", t2.f0, t2.f1));
            }
        }
        schemaSB.append(String.format("%s %s", HUDI_DDL_BASE_SCHEMA.get(HUDI_DDL_BASE_SCHEMA.size() - 1).f0, HUDI_DDL_BASE_SCHEMA.get(HUDI_DDL_BASE_SCHEMA.size() - 1).f1));
        pro.forEach((k, v) -> {
            proSB.append(String.format(",'%s'='%s'\n", k, v));
        });
        return String.format("create table %s (\n" +
                "%s" +
                ") with (\n" +
                "  'connector' = 'socket'" +
                "  %s \n" +
                ")", tblName, schemaSB, proSB);
    }

    public static String PRINT_DDL(String tblName, ArrayList<Tuple2<String, String>> schema) {
        StringBuilder schemaSB = new StringBuilder();
        if (schema != null)
            HUDI_DDL_BASE_SCHEMA = schema;
        for (int i = 0; i < HUDI_DDL_BASE_SCHEMA.size(); i++) {
            Tuple2<String, String> t2 = HUDI_DDL_BASE_SCHEMA.get(i);
            if (i < HUDI_DDL_BASE_SCHEMA.size() - 1) {
                schemaSB.append(String.format("%s %s,\n", t2.f0, t2.f1));
            }
        }
        schemaSB.append(String.format("%s %s", HUDI_DDL_BASE_SCHEMA.get(HUDI_DDL_BASE_SCHEMA.size() - 1).f0, HUDI_DDL_BASE_SCHEMA.get(HUDI_DDL_BASE_SCHEMA.size() - 1).f1));

        return String.format("create table %s (\n" +
                "%s" +
                ") with (\n" +
                "  'connector' = 'print'" +
                ")", tblName, schemaSB);
    }

    public static void main(String[] args) {

    }
}
