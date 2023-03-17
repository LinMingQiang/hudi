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

public class ITTestSparkSQLSyncHive extends ITSparkUtils {
    static {
        enableHive = true;
    }
    String CREATE_DDL = String.format("create table %s (\n" +
            "  id int," +
            " ts int" +
            ") using hudi\n" +
            " tblproperties (\n" +
            "  type = 'mor',\n" +
            "  primaryKey = 'id',\n" +
            "  preCombineField = 'ts'," +
            "  hoodie.datasource.hive_sync.enable = 'true'," +
            "  hoodie.datasource.hive_sync.table.strategy='ro'" +
            "\n" +
            ")"
            +" location '%s' ",
            TBL_NAME, PATH);
    /**
     *
     */
    @Test
    public void testHoodieSparkQuickstart() {
        spark.sql(CREATE_DDL);
        spark.table("hudi_mor_tbl").printSchema();
        spark.sql("insert into test.spark_sql_hive_sync values (1, 1)");
    }


    /**
     * 当开启hive同步的时候，同时配置了 hoodie.datasource.hive_sync.table.strategy。
     * 1：假设spark 建了一个hudi表 A 到了hive，这个时候hive存在了表 A，但是这个表A的结构是 rt表的性质，因为他的 inputformat是 HoodieParquetRealtimeInputFormat
     * 2：当写完数据后，hive会同步 ro表，但是这个ro表是没有后缀的，所以也是同步的 A 表
     * 3：但是由于A存在，所以并不会对表A进行任何更改
     * 4：导致表A 是一个rt表。
     *
     * 'hoodie.query.as.ro.table'='true', 决定了spakr读取的时候是ro还是rt，
     * STORED AS INPUTFORMAT 决定了hive是读的ro还是rt
     */
    @Test
    public void testSparkSQLHiveSync() {
        spark.sql("create table test.spark_sql_hive_sync (\n" +
                 "  id int," +
                 " age int," +
                 " ts int" +
                ") using hudi\n" +
                " tblproperties (\n" +
                "  type = 'mor',\n" +
                "  primaryKey = 'id',\n" +
                "  preCombineField = 'ts'," +
                "  hoodie.datasource.hive_sync.enable = 'true'" +
//                "  ,hoodie.datasource.hive_sync.table.strategy='ro'" +
                "\n" +
                ")"
                +" location '/Users/hunter/workspace/lmq/hudi-master-debug/hudi-debug/hudi-debug-spark/target/TestSparkSQL/hudi_mor_tbl' "
        );
        spark.table("hudi_mor_tbl").printSchema();

        spark.sql("merge into test.spark_sql_hive_sync as target " +
                " using (select 1 as id, 1 as age, 1 as int ) as source " +
                " ON target.`id` = source.`id` " +
                "WHEN MATCHED THEN UPDATE SET * " +
                "WHEN NOT MATCHED THEN INSERT * ");


        spark.sql("merge into test.spark_sql_hive_sync as target " +
                " using (select 1 as id, 3 as age, 3 as int ) as source " +
                " ON target.`id` = source.`id` " +
                "WHEN MATCHED THEN UPDATE SET * " +
                "WHEN NOT MATCHED THEN INSERT * ");

        spark.sql("merge into test.spark_sql_hive_sync as target " +
                " using (select 1 as id, 4 as age, 2 as int ) as source " +
                " ON target.`id` = source.`id` " +
                "WHEN MATCHED THEN UPDATE SET * " +
                "WHEN NOT MATCHED THEN INSERT * ");
//
//        spark.sql("merge into test.spark_sql_hive_sync as target " +
//                " using (select 2 as id, 2 as int ) as source " +
//                " ON target.`id` = source.`id` " +
//                "WHEN MATCHED THEN UPDATE SET * " +
//                "WHEN NOT MATCHED THEN INSERT * ");
//        spark.sql("merge into test.spark_sql_hive_sync as target " +
//                " using (select 3 as id, 2 as int ) as source " +
//                " ON target.`id` = source.`id` " +
//                "WHEN MATCHED THEN UPDATE SET * " +
//                "WHEN NOT MATCHED THEN INSERT * ");
//        spark.sql("merge into test.spark_sql_hive_sync as target " +
//                " using (select 4 as id, 2 as int ) as source " +
//                " ON target.`id` = source.`id` " +
//                "WHEN MATCHED THEN UPDATE SET * " +
//                "WHEN NOT MATCHED THEN INSERT * ");
//        spark.sql("merge into test.spark_sql_hive_sync as target " +
//                " using (select 5 as id, 2 as int ) as source " +
//                " ON target.`id` = source.`id` " +
//                "WHEN MATCHED THEN UPDATE SET * " +
//                "WHEN NOT MATCHED THEN INSERT * ");
    }
}
