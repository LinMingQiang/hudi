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

/**
 * 多流拼接
 * 1：如果两次写入的 schema不一样，在查询的时候是使用最近写入的schema，这样导致查出来的结果不对
 * 2：修改的方式可以在查询的时候选择schema的时候从Properties文件里面拿
 *
 * 注意，建表的时候需要建全部字段，
 */
public class ITTestPartialPayload extends TestUtil {
    public static String TABLE_NAME = "ITTestPartialPayload";

    static {
        SINK_PRO.put("payload.class", "org.apache.hudi.common.model.PartialUpdateAvroPayload");
    }

    /**
     * 1,name,1,1,par1
     * 1,,2,2,par1
     * -- 1,name,2,2,par1
     * 建表全字段，否则下面的写不进去.
     * @throws Exception
     */
    @Test
    public void testWriteAllCols() throws Exception {
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, age, ts, par)  values('1',2,1,'par1')").await();
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, name, ts, par) values('1','name',1,'par1')").await();
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, age, ts, par)  values('1',3,1,'par1')").await();

    }


    /**
     * 去掉一个字段age
     *
     * @throws Exception
     */
    @Test
    public void testWriteB() throws Exception {
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("name", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, name, ts, par) values('1','name',2,'par1')").await();

    }

    /**
     * 不能定义不全的schema，会导致读取报错
     * @throws Exception
     */
    @Test
    public void testWriteC() throws Exception {
        SINK_SCHEMA.add(new Tuple2<>("id", "STRING"));
        SINK_SCHEMA.add(new Tuple2<>("age", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("ts", "bigint"));
        SINK_SCHEMA.add(new Tuple2<>("`par`", "STRING"));
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.executeSql("insert into "+TABLE_NAME+"(id, age, ts, par)  values('1',3,1,'par1')").await();

    }

    @org.junit.Test
    public void testRead() throws Exception {
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
