package com.vip.hudi.flink.mor.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class TestUtil {
    public static StreamTableEnvironment tableEnv = null;
    public static StreamExecutionEnvironment env = null;

    public static String FILE_HUDI_PATH = "file://" + System.getProperty("user.dir") + "/target/";

    public static String FILE_SRC_PATH = "file://" + System.getProperty("user.dir") + "/src/test/resources/testData";

    static {
        if(System.getProperty("file.separator").startsWith("\\")) {
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
            "  'path' = '"+FILE_SRC_PATH+"',\n" +
            "  'format' = 'csv'\n" +
            ")";

    public String FILE_SRC_HUDI_TBL(String path){
        return "CREATE TABLE file_src_hudi_tbl(\n" +
                "    id STRING PRIMARY KEY NOT ENFORCED,\n" +
                "   `name` STRING,\n" +
                "    age bigint,\n" +
                "    rowtime STRING,\n" +
                "    `dt` STRING\n" +
                ") PARTITIONED BY (`dt`) WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'path' = '"+FILE_HUDI_PATH + path +"',\n" +
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

    static{
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(5000L);
    }
    public static void init(RuntimeExecutionMode mode){
        env.setRuntimeMode(mode);
        tableEnv = StreamTableEnvironment.create(env);
    }


    public static String readFile(String filePath) {
        if(filePath == null){
            return null;
        }
        StringBuffer stringBuffer = null;
        BufferedReader bufferedReader = null;
        InputStream is = null;
        try {

            is = TestUtil.class.getClassLoader().getResourceAsStream(filePath);

            if(is == null ){
                is =  new FileInputStream(filePath);
            }

            if(is == null){
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
