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

public class ITTestSparkSQLQuickstart implements SparkProvider {
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

}
