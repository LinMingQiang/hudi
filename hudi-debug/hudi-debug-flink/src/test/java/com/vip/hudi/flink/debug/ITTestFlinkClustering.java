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

package com.vip.hudi.flink.debug;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import static com.vip.hudi.flink.utils.TestSQLManager.SRC_SOCKET_DDL;

public class ITTestFlinkClustering extends TestUtil {
    public static String TABLE_NAME = "ITTestFlinkClustering";

    public static String SRC_TABLE_NAME = "socket";

    static {
        SINK_PRO.put("payload.class", "org.apache.hudi.common.model.PartialUpdateAvroPayload");
        SINK_PRO.put("clustering.schedule.enabled", "true");
        SINK_PRO.put("clustering.async.enabled", "true");
        SINK_PRO.put("clustering.delta_commits", "2");

        SRC_PRO.put("hostname", "localhost");
        SRC_PRO.put("port", "9877");
        SRC_PRO.put("format", "csv");
    }

    /**
     * 1,name,1,1,par1
     * 1,,2,2,par1
     * -- 1,name,2,2,par1
     * 建表全字段，否则下面的写不进去.
     * @throws Exception
     */
    @Test
    public void testSimpleDemoBatch() throws Exception {
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, age, ts, par)  values('1',2,2,'par1')").await();
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, name, ts, par) values('1','name',1,'par1')").await();
    }


    // 1,name,1,par1
    @Test
    public void testSimpleDemoStream() throws Exception {
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "string"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.STREAMING, TABLE_NAME);
        SRC_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SRC_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SRC_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SRC_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        System.out.println(SRC_SOCKET_DDL(SRC_TABLE_NAME, SRC_SCHEMA, SRC_PRO));
        tableEnv.executeSql(SRC_SOCKET_DDL(SRC_TABLE_NAME, SRC_SCHEMA, SRC_PRO));
        tableEnv.executeSql("insert into "+TABLE_NAME+" select id, name, age, CAST(NOW() as String) as ts, par from " + SRC_TABLE_NAME).await();
    }

    @org.junit.Test
    public void testRead() throws Exception {
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "string"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.sqlQuery("select * from " + TABLE_NAME +
                "/*+ OPTIONS(" +
                "'read.streaming.enabled' = 'false'," +
                "'read.tasks'='1'," +
                "'read.start-commit'='0'," +
                "'read.end-commit'='20231013101032156') */"
        ).execute().print();
    }
}
