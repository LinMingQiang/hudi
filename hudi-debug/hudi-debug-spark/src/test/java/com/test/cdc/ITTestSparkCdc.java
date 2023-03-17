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
package com.test.cdc;

import com.test.utils.ITSparkUtils;
import org.junit.jupiter.api.Test;

public class ITTestSparkCdc extends ITSparkUtils {
    static {
        TBL_NAME = "ITTestSparkCdc";
        init();
        CREATE_DDL = String.format("create table %s (\n" +
                        "  id int," +
                        " ts int" +
                        ") using hudi\n" +
                        " tblproperties (\n" +
                        "  type = 'mor',\n" +
                        "  primaryKey = 'id',\n" +
                        "  preCombineField = 'ts'," +
                        "  hoodie.table.cdc.enabled='true'," +
                        "  hoodie.datasource.query.type = 'incremental'," +
                        "  hoodie.datasource.query.incremental.format = 'cdc'," +
                        "  hoodie.datasource.read.begin.instanttime='0'" +
                        "\n" +
                        ")"
                        +" location '%s' ",
                TBL_NAME, PATH);
    }

    @Test
    public void testBatchSparkWrite() {
//        SINK_PRO.put("hoodie.datasource.query.type", "incremental");
//        SINK_PRO.put("hoodie.datasource.query.incremental.format", "cdc");
//        SINK_PRO.put("hoodie.table.cdc.supplemental.logging.mode", "data_before_after");
        spark.sql(CREATE_DDL);
        spark.table(TBL_NAME).printSchema();
        spark.sql(String.format("insert into %s values (1, 2)", TBL_NAME));
        spark.sql(String.format("insert into %s values (1, 3)", TBL_NAME));
        spark.sql(String.format("insert into %s values (1, 4)", TBL_NAME));

    }

    @Test
    public void testRead() {
//        SINK_PRO.put("hoodie.datasource.query.type", "incremental");
//        SINK_PRO.put("hoodie.datasource.query.incremental.format", "cdc");
//        SINK_PRO.put("hoodie.table.cdc.supplemental.logging.mode", "data_before_after");
        spark.sql(CREATE_DDL);
        spark.sql(String.format("select * from %s ",TBL_NAME)).show();
    }
}
