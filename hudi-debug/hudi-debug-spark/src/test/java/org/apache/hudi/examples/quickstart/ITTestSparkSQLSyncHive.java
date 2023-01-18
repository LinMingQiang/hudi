package org.apache.hudi.examples.quickstart;

import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.testutils.providers.SparkProvider;
import org.apache.spark.HoodieSparkKryoProvider$;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Paths;

public class ITTestSparkSQLSyncHive implements SparkProvider {
    protected static HoodieSparkEngineContext context;

    private static SparkSession spark;
    private static SQLContext sqlContext;
    private static JavaSparkContext jsc;

    /**
     * An indicator of the initialization status.
     */
    protected boolean initialized = false;
    @TempDir
    protected java.nio.file.Path tempDir;

    @Override
    public SparkSession spark() {
        return spark;
    }

    @Override
    public SQLContext sqlContext() {
        return sqlContext;
    }

    @Override
    public JavaSparkContext jsc() {
        return jsc;
    }

    @Override
    public HoodieSparkEngineContext context() {
        return context;
    }

    public String basePath() {
        return tempDir.toAbsolutePath().toString();
    }

    public String tablePath(String tableName) {
        return Paths.get(basePath(), tableName).toString();
    }

    @BeforeEach
    public synchronized void runBeforeEach() {
        initialized = spark != null;
        if (!initialized) {
            SparkConf sparkConf = conf();
            HoodieSparkKryoProvider$.MODULE$.register(sparkConf);
            SparkRDDReadClient.addHoodieSupport(sparkConf);

            spark = SparkSession.builder()
                    .enableHiveSupport()
                    .master("local[1]")
                    .appName("hoodie sql test")
                    .withExtensions(new HoodieSparkSessionExtension())
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("hoodie.insert.shuffle.parallelism", "4")
                    .config("hoodie.upsert.shuffle.parallelism", "4")
                    .config("hoodie.delete.shuffle.parallelism", "4")
//                    .config("spark.sql.warehouse.dir", "/Users/hunter/workspace/lmq/hudi-master-debug/hudi-debug/hudi-debug-spark/target/TestSparkSQL")
                    .config("spark.sql.session.timeZone", "UTC")
                    .config(sparkConf)
                    .getOrCreate();
            sqlContext = spark.sqlContext();
            jsc = new JavaSparkContext(spark.sparkContext());
            context = new HoodieSparkEngineContext(jsc);
        }
    }

    /**
     *
     */
    @Test
    public void testHoodieSparkQuickstart() {
        String tableName = "spark_quick_start";
        spark.sql("create table test.spark_sql_hive_sync (\n" +
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
                +" location '/Users/hunter/workspace/lmq/hudi-master-debug/hudi-debug/hudi-debug-spark/target/TestSparkSQL/hudi_mor_tbl' "
        );
        spark.table("hudi_mor_tbl").printSchema();

        spark.sql("insert into test.spark_sql_hive_sync values (1, 1)");
    }


    /**
     * 当开启hive同步的时候，同时配置了 hoodie.datasource.hive_sync.table.strategy。
     * 1：假设spark 建了一个hudi表 A 到了hive，这个时候hive存在了表 A，但是这个表A的结构是 rt表的性质，因为他的 inputformat是 HoodieParquetRealtimeInputFormat
     * 2：当写完数据后，hive会同步 ro表，但是这个ro表是没有后缀的，所以也是同步的 A 表
     * 3：但是由于A存在，所以并不会对表A进行任何更改
     * 4：导致表A 是一个rt表。
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
