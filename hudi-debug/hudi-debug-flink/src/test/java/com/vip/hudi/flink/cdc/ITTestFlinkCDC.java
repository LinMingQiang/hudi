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
package com.vip.hudi.flink.cdc;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

public class ITTestFlinkCDC extends TestUtil {
    public static String TABLE_NAME = "ITTestFlinkCDC";

    static {
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "string"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));

        SINK_PRO.put("cdc.enabled", "true");
        SINK_PRO.put("payload.class", "org.apache.hudi.common.model.PartialUpdateAvroPayload");
//        SINK_PRO.put("hoodie.datasource.query.type", "incremental");
//        SINK_PRO.put("hoodie.datasource.query.incremental.format", "cdc");
        SINK_PRO.put("cdc.supplemental.logging.mode", "data_before_after");

        SRC_PRO.put("hostname", "localhost");
        SRC_PRO.put("port", "9877");
        SRC_PRO.put("format", "csv");
    }

    /**
     * 看代码 ，只支持 cow？ FlinkWriteHandleFactory#getFactory
     * @throws Exception
     */
    @Test
    public void testSimpleDemoBatchMOR() throws Exception {
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.executeSql("insert into " + TABLE_NAME + "(id, name, age, ts, par)  values('1','name3',2,'2','par1')").await();
        tableEnv.executeSql("insert into " + TABLE_NAME + "(id, name, age, ts, par)  values('1','name3',7,'2','par1')").await();
        tableEnv.executeSql("insert into " + TABLE_NAME + "(id, name, age, ts, par)  values('1','name4',4,'4','par1')").await();
        tableEnv.executeSql("insert into " + TABLE_NAME + "(id, name, age, ts, par)  values('1','name5',5,'5','par1')").await();
    }


    @Test
    public void testSimpleDemoBatchCOW() throws Exception {
        initEnvAndDefaultCOW(TABLE_NAME);
        tableEnv.executeSql("insert into " + TABLE_NAME + "(id, name, age, ts, par)  values('1','name3',2,'2','par1')").await();
        tableEnv.executeSql("insert into " + TABLE_NAME + "(id, name, age, ts, par)  values('1','name3',7,'2','par1')").await();
        tableEnv.executeSql("insert into " + TABLE_NAME + "(id, name, age, ts, par)  values('1','name4',4,'4','par1')").await();
        tableEnv.executeSql("insert into " + TABLE_NAME + "(id, name, age, ts, par)  values('1','name5',5,'5','par1')").await();

    }
    /**
     * cdc read 只支持 streaming模式
     * @throws Exception
     */
    @Test
    public void testRead() throws Exception {
        isStreaming = true;
        initEnvAndDefaultCOW(TABLE_NAME);
//        tableEnv.executeSql(PRINT_DDL("printTbl", SINK_SCHEMA));
        tableEnv.sqlQuery(String.format("select * from " + TABLE_NAME +
                "/*+ OPTIONS(" +
                "'read.streaming.enabled' = '%s'," +
                "'read.tasks'='1'," +
                "'read.start-commit'='0'," +
                "'read.end-commit'='20241013101032156') */", RUN_MODE.f1)
        ).execute().print();
    }
}
