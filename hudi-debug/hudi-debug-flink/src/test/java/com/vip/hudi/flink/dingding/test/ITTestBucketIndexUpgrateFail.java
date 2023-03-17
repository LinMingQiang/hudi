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
package com.vip.hudi.flink.dingding.test;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Random;

public class ITTestBucketIndexUpgrateFail extends TestUtil {
    // bucket index
    String ddl = "CREATE TABLE hudi_4726(" +
            "                                     id string,\n" +
            "                                     msg string,\n" +
            "                                     `partition` STRING,\n" +
            "                                     PRIMARY KEY(id) NOT ENFORCED\n" +
            ") PARTITIONED BY (`partition`)\n" +
            "    WITH (\n" +
            "        'connector' = 'hudi',\n" +
            "'write.parquet.max.file.size'='1'," +
            "'write.log.max.size'='1'," +
            "        'write.operation'='upsert',\n" +
            "        'path' = 'file:///Users/hunter/workspace/hudipr/release-0.11.0/hudi-examples/hudi-examples-debug/target/ITTestBucketIndexUpgrateFail',\n" +
            "        'index.type' = 'BUCKET',\n" +
            "        'hoodie.bucket.index.num.buckets' = '10'," +
            "        'compaction.delta_commits' = '10'," +
            "        'table.type' = 'MERGE_ON_READ'," +
            "        'compaction.async.enabled'='true'" +
            ")";

    @Test
    public void testtWrite() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        tableEnv.executeSql(ddl);
        System.out.println(ddl);
        Random r = new Random();
        while (true) {
            String insert = "INSERT INTO hudi_4726 values ('id1','t1','par1'),";
            for (int i = 0; i < 100; i++) {
                insert += "('id" + r.nextInt(1000) + "','t1','par" + r.nextInt(11) + "'),";
            }
            insert += "('id10','t1','par" + 1 + "')";
            tableEnv.executeSql(insert).await();
        }
    }


    @org.junit.Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        tableEnv.executeSql(ddl);
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from hudi_4726 " +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20230827020134588') */"
                ), Row.class)
                .print();

        env.execute();
    }
}
