/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.vip.hudi.flink.utils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TestUtil {
    public static StreamTableEnvironment tableEnv = null;
    public static StreamExecutionEnvironment env = null;
    public static String FILE_HUDI_PATH = "file://" + System.getProperty("user.dir") + "/target/";
    public static String HUDI_DDL;
    public static HashMap<String, String> SRC_PRO = new HashMap<>();
    public static HashMap<String, String> SINK_PRO = new HashMap<>();
    public static ArrayList<Tuple2<String, String>> SRC_SCHEMA = new ArrayList<>();
    public static ArrayList<Tuple2<String, String>> SINK_SCHEMA = new ArrayList<>();


    public static boolean isStreaming = false;
    public static Tuple2<RuntimeExecutionMode, String> RUN_MODE = isStreaming ? Tuple2.of(RuntimeExecutionMode.STREAMING, "true") : Tuple2.of(RuntimeExecutionMode.BATCH, "false");

    public static String FILE_SRC_PATH = "file://" + System.getProperty("user.dir") + "/src/test/resources/testData";

    static {
        if (System.getProperty("file.separator").startsWith("\\")) {
            System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0");
            FILE_HUDI_PATH = System.getProperty("user.dir") + "\\target\\";
            FILE_SRC_PATH = System.getProperty("user.dir") + "\\src\\test\\resources\\testData";
        }
    }


    public static String FILE_SRC_TBL = "create table file_src_tbl (\n" +
            "    id VARCHAR,\n" +
            "    name varchar,\n" +
            "    age int,\n" +
            "    rowtime TIMESTAMP(3),\n" +
            "    `dt` as DATE_FORMAT(rowtime, 'yyyy-MM-dd')\n" +
            ") with (\n" +
            "  'connector' = 'filesystem',\n" +
            "  'path' = '" + FILE_SRC_PATH + "',\n" +
            "  'format' = 'csv'\n" +
            ")";


    public String HUDI_MOR_TBL(String path) {
        return "CREATE TABLE HUDI_MOR_TBL(\n" +
                "    id STRING PRIMARY KEY NOT ENFORCED,\n" +
                "   `name` STRING,\n" +
                "    age bigint,\n" +
                "    rowtime STRING,\n" +
                "    `dt` STRING\n" +
                ") PARTITIONED BY (`dt`) WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'path' = '" + FILE_HUDI_PATH + path + "',\n" +
                "    'write.payload.class' = 'org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload',\n" +
                "    'write.precombine' = 'true',\n" +
                "    'write.precombine.field' = 'rowtime',\n" +
                "    'compaction.delta_commits' = '1',\n" +
                "    'compaction.schedule.enable' = 'true',\n" +
                "    'compaction.async.enabled' = 'true',\n" +
                "    'changelog.enabled' = 'false',\n" +
                "    'index.type' = 'BUCKET', \n" +
                "    'hoodie.bucket.index.num.buckets'='1', \n" +
                "    'write.tasks' = '1')\n";
    }


    public static String KAFKA_SRC_TBL = "CREATE TABLE kafka_src_tbl (\n" +
            "    `timestamp` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
            "    topic VARCHAR METADATA FROM 'topic',\n" +
            "    `offset` bigint METADATA,\n" +
            "    id VARCHAR,\n" +
            "    name varchar,\n" +
            "    age int,\n" +
            "    rowtime TIMESTAMP(3),\n" +
            "    `dt` as DATE_FORMAT(rowtime, 'yyyy-MM-dd'),\n" +
            "    WATERMARK FOR rowtime AS rowtime - INTERVAL '10' SECOND\n" +
            ") WITH (\n" +
            "    'connector' = 'kafka',\n" +
            "    'topic' = 'test',\n" +
            "    'scan.startup.mode' = 'latest-offset',\n" +
            "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "    'properties.group.id' = 'test',\n" +
            "    'format' = 'csv'\n" +
            ")";

    static {

    }

    public void initEnvAndDefaultCOW( String tableName) {
        RUN_MODE = isStreaming ? Tuple2.of(RuntimeExecutionMode.STREAMING, "true") : Tuple2.of(RuntimeExecutionMode.BATCH, "false");
        initEnv(RUN_MODE.f0);
        initCOW(tableName);
    }
    public void initCOW(String tableName) {
        HUDI_DDL = TestSQLManager.HUDI_DDL(
                tableName,
                "COPY_ON_WRITE",
                FILE_HUDI_PATH + tableName,
                SINK_PRO,
                SINK_SCHEMA);
        System.out.println(HUDI_DDL);
        tableEnv.executeSql(HUDI_DDL);
    }

    public void initMOR(String tableName) {
        HUDI_DDL = TestSQLManager.HUDI_DDL(
                tableName,
                "MERGE_ON_READ",
                FILE_HUDI_PATH + tableName,
                SINK_PRO,
                SINK_SCHEMA);
        System.out.println(HUDI_DDL);
        tableEnv.executeSql(HUDI_DDL);
    }

    public void initEnvAndDefaultMOR(RuntimeExecutionMode mode, String tableName) {
        initEnv(mode);
        initMOR(tableName);
    }

    public void initEnvAndDefaultMOR( String tableName) {
         RUN_MODE = isStreaming ? Tuple2.of(RuntimeExecutionMode.STREAMING, "true") : Tuple2.of(RuntimeExecutionMode.BATCH, "false");
        initEnv(RUN_MODE.f0);
        initMOR(tableName);
    }

    public void initEnv(RuntimeExecutionMode mode) {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(1000L);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(5000L);
        env.setRuntimeMode(mode);
        tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
    }

    public static String DATAGEN_SRC_TBL(boolean isStreaming) {
        String s = "'number-of-rows' = '5'";
        if (isStreaming) {
            s = "'rows-per-second' = '5'";
        }
        return "create table DATAGEN_SRC_TBL (\n" +
                "    id VARCHAR,\n" +
                "    name varchar,\n" +
                "    age int,\n" +
                "    rowtime TIMESTAMP(3),\n" +
                "    `dt` as DATE_FORMAT(rowtime, 'yyyy-MM-dd')\n" +
                ") with (\n" +
                "  'connector' = 'datagen',\n" + s +
                ")";
    }

    public static String readFile(String filePath) {
        if (filePath == null) {
            return null;
        }
        StringBuffer stringBuffer = null;
        BufferedReader bufferedReader = null;
        InputStream is = null;
        try {

            is = TestUtil.class.getClassLoader().getResourceAsStream(filePath);

            if (is == null) {
                is = new FileInputStream(filePath);
            }

            if (is == null) {
                return null;
            }

            bufferedReader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            stringBuffer = new StringBuffer();
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                if ("\r".equals(line)) {
                    continue;
                }
                stringBuffer.append(line).append("\n");
            }
        } catch (Exception e) {
            return null;
        } finally {
            closeBufferedReader(bufferedReader);
        }
        return stringBuffer.toString();
    }

    public static void closeBufferedReader(BufferedReader bufferedReader) {
        if (bufferedReader != null) {
            try {
                bufferedReader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
