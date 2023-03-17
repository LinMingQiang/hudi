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
package com.vip.hudi.flink.spark;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

/**
 * spark先写到hoodie，然后同步表A （ro）。
 *  --
 * 然后flink再写入，同步表A （ro）。
 * 1：如果flink写的字段顺序和spark的顺序不一致可以吗
 */
public class ITTestSparkWriteThenFlinkWrite extends TestUtil {
    public static String TABLE_NAME = "ITTestSparkWriteThenFlinkWrite";

    public static String SRC_TABLE_NAME = "socket";

    static {
        SINK_PRO.put("payload.class", "org.apache.hudi.common.model.PartialUpdateAvroPayload");
        SRC_PRO.put("hostname", "localhost");
        SRC_PRO.put("port", "9877");
        SRC_PRO.put("format", "csv");
    }

    @Test
    public void testSimpleDemoBatch() throws Exception {
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
//        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, age, name, ts, par)  values('2',2,'name',2,'par1')").await();
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, name, age, ts, par)  values('5','name4',4,4,'par1')").await();

    }


    @org.junit.Test
    public void testRead() throws Exception {
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.sqlQuery("select id,name,age,ts from " + TABLE_NAME +
                "/*+ OPTIONS(" +
                "'read.streaming.enabled' = 'false'," +
                "'read.tasks'='1'," +
                "'read.start-commit'='0'," +
                "'read.end-commit'='20231013101032156') */"
        ).execute().print();
    }
}