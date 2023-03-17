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
import org.junit.jupiter.api.Test;

public class ITTestBulkInsert extends TestUtil {
    @Test
    public void testBulkInsertWrite() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        String sqls = readFile("bulkinsert.sql");
        for (String statement : sqls.split(";")) {
            if (!statement.trim().isEmpty()) {
                if (statement.contains("INSERT")) {
                    System.out.println(statement);
                    tableEnv.executeSql(statement).await();
                } else {
                    System.out.println(statement);
                    tableEnv.executeSql(statement);
                }
            }
        }
        tableEnv.executeSql("INSERT INTO bulk_insert_test_mor\n" +
                "                select\n" +
                "        binlog_event_type,\n" +
                "                binlog_create_time,\n" +
                "                binlog_update_time,\n" +
                "                binlog_is_deleted,\n" +
                "                binlog_schema_name,\n" +
                "                binlog_table_name,\n" +
                "                binlog_position,\n" +
                "                order_id,\n" +
                "                create_time,\n" +
                "                customer_name,\n" +
                "                price,\n" +
                "                product_id,\n" +
                "                order_status,\n" +
                "                last_update_time,\n" +
                "                dt\n" +
                "        from bulk_insert_src").await();

        tableEnv.executeSql("INSERT INTO bulk_insert_test_mor\n" +
                "/*+ OPTIONS('write.operation'='upsert') */" +
                "values ('a',1,2,1,'a','a',1,1,'2022-01-01 11:11:12','b',1.1,1,1,'2022-01-01 11:11:12','20220101')").await();
        // 有两个版本，一个是bulkinsert 写入，一个是 正常写入.
    }

    @Test
    public void testAfterBulkInsertWrite() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        String sqls = readFile("bulkinsert.sql");
        for (String statement : sqls.split(";")) {
            if (!statement.trim().isEmpty()) {
                System.out.println(statement);
                tableEnv.executeSql(statement);
            }
        }
        tableEnv.executeSql("INSERT INTO bulk_insert_test_mor\n" +
                "/*+ OPTIONS('write.operation'='upsert') */" +
                "values ('a',1,2,1,'a','a',1,1,'2022-01-01 11:11:12','b',1.1,1,1,'2022-01-01 11:11:12','20220101')").await();
        tableEnv.executeSql("INSERT INTO bulk_insert_test_mor\n" +
                "/*+ OPTIONS('write.operation'='upsert') */" +
                "values ('b',1,2,1,'b','b',1,1,'2022-01-01 11:11:14','b',1.1,1,1,'2022-01-01 11:11:14','20220101')").await();
        tableEnv.executeSql("INSERT INTO bulk_insert_test_mor\n" +
                "/*+ OPTIONS('write.operation'='upsert') */" +
                "values ('c',1,2,1,'c','c',1,1,'2022-01-01 11:11:14','b',1.1,1,1,'2022-01-01 11:11:14','20220101')").await();
        tableEnv.executeSql("INSERT INTO bulk_insert_test_mor\n" +
                "/*+ OPTIONS('write.operation'='upsert') */" +
                "values ('d',1,2,1,'d','d',1,1,'2022-01-01 11:11:14','b',1.1,1,1,'2022-01-01 11:11:14','20220101')").await();

    }

    @org.junit.Test
    public void testRead() throws Exception {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        String sqls = readFile("bulkinsert.sql");
        for (String statement : sqls.split(";")) {
            if (!statement.trim().isEmpty()) {
                System.out.println(statement);
                tableEnv.executeSql(statement);
            }
        }
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from bulk_insert_test_mor " +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20220826102700570') */"
                ), Row.class)
                .print();
        env.execute();
    }
}
