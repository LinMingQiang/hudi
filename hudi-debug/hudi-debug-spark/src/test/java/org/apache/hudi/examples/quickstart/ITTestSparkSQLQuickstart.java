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
package org.apache.hudi.examples.quickstart;

import com.test.utils.ITSparkUtils;
import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.testutils.providers.SparkProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Paths;

public class ITTestSparkSQLQuickstart extends ITSparkUtils {
    static {
        TBL_NAME = "ITTestSparkSQLQuickstart";
        PATH = HUDI_PATH + TBL_NAME;
        CREATE_DDL = String.format("create table %s (\n" +
                        "  id int," +
                        " ts int" +
                        ") using hudi\n" +
                        " tblproperties (\n" +
                        "  type = 'mor',\n" +
                        "  primaryKey = 'id',\n" +
                        "  preCombineField = 'ts'" +
                        "\n" +
                        ")"
                        +" location '%s' ",
                TBL_NAME, PATH);
    }

    @Test
    public void testWrite() {
        spark.sql(CREATE_DDL);
        spark.sql(String.format("insert into %s values (1, 1)", TBL_NAME));
    }

    @Test
    public void testRead() {
        System.out.println(CREATE_DDL);
        spark.sql(CREATE_DDL);
        spark.table(TBL_NAME).show();
    }
}
