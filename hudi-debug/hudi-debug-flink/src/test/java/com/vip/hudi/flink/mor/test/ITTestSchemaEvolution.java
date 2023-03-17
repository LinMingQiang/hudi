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
package com.vip.hudi.flink.mor.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;

import java.util.concurrent.ExecutionException;

public class ITTestSchemaEvolution extends TestUtil {
    public String tblName = "ITTestDynamicSchema";

    public String HUDI_MOR_TBL2(String path){
        return "CREATE TABLE HUDI_MOR_TBL2(\n" +
                "    id STRING PRIMARY KEY NOT ENFORCED,\n" +
                "   `name` STRING,\n" +
                "    age int,\n" +
                "    rowtime STRING,\n" +
                "   `name2` STRING,\n" +
                "    `dt` STRING\n" +
                ") PARTITIONED BY (`dt`) WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'path' = '"+FILE_HUDI_PATH + path +"',\n" +
                "    'write.payload.class' = 'org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload',\n" +
                "    'write.precombine' = 'true',\n" +
                "    'write.precombine.field' = 'rowtime',\n" +
                "    'compaction.delta_commits' = '5',\n" +
                "    'compaction.schedule.enable' = 'true',\n" +
                "    'compaction.async.enabled' = 'true',\n" +
                "    'changelog.enabled' = 'false',\n" +
                "    'index.type' = 'BUCKET', \n" +
                "    'hoodie.bucket.index.num.buckets'='1', \n" +
                "    'write.tasks' = '1')\n";
    }
    @org.junit.Test
    public void testWrite() throws ExecutionException, InterruptedException {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(FILE_SRC_TBL);


        tableEnv.executeSql(HUDI_MOR_TBL(tblName));
        String insertSql = "insert into HUDI_MOR_TBL " +
                " select " +
                "id," +
                "name," +
                "age," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd HH:mm:ss') as rowtime," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd') as dt" +
                " from file_src_tbl";
        tableEnv.executeSql(insertSql).await();


        tableEnv.executeSql(HUDI_MOR_TBL2(tblName));
        String insertSql2 = "insert into HUDI_MOR_TBL2 " +
                " select " +
                "id," +
                "name," +
                "age," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd HH:mm:ss') as rowtime," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd') as dt" +
                " from file_src_tbl";
        tableEnv.executeSql(insertSql2).await();

    }

    @org.junit.Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(HUDI_MOR_TBL2(tblName));
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from HUDI_MOR_TBL2"+
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'write.precombine' = 'true',"+
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20220818103225741') */"), Row.class)
                .print();
        env.execute();
    }
}
