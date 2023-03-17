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
package com.vip.hudi.flink.pr;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;

import java.util.concurrent.ExecutionException;

public class PR_4961 extends TestUtil {
    // https://github.com/apache/hudi/pull/6845
    // batch支持clean
    String tblName = "PR_4945";


    @org.junit.Test
    public void testWrite() throws ExecutionException, InterruptedException {
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(DATAGEN_SRC_TBL(false));
        tableEnv.executeSql(HUDI_MOR_TBL(tblName));
        String insertSql = "insert into HUDI_MOR_TBL " +
                "/*+ OPTIONS(" +
                "'clean.retain_commits'='1'," +
                "    'hive_sync.enable' = 'true',\n" +
                "    'hive_sync.mode' = 'hms',\n" +
                "    'hive_sync.metastore.uris' = 'thrift://localhost:9083',\n" + //bigdata-hiveserver.vip.vip.com
                "    'hive_sync.db' = 'test',\n" +
                "    'hive_sync.table' = 'PR_4961',"+
                "    'hive_sync.table.strategy'='ONLY_Rt'" +
                ") */" +
                " select " +
                "id," +
                "name," +
                "age," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd HH:mm:ss') as rowtime," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd') as dt" +
                " from DATAGEN_SRC_TBL";

        tableEnv.executeSql(insertSql).await();
    }


    @org.junit.Test
    public void testStreamingWrite() throws ExecutionException, InterruptedException {
        initEnv(RuntimeExecutionMode.STREAMING);
        tableEnv.executeSql(DATAGEN_SRC_TBL(true));
        tableEnv.executeSql(HUDI_MOR_TBL(tblName));
        String insertSql = "insert into HUDI_MOR_TBL " +
                "/*+ OPTIONS(" +
                "'clean.retain_commits'='1'" +
                ") */" +
                " select " +
                "id," +
                "name," +
                "age," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd HH:mm:ss') as rowtime," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd') as dt" +
                " from DATAGEN_SRC_TBL";

        tableEnv.executeSql(insertSql).await();
    }
}