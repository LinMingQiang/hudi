package com.test.utils;

import org.apache.hudi.client.SparkRDDReadClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.testutils.providers.SparkProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Paths;

public class ITSparkUtils implements SparkProvider {
    protected static HoodieSparkEngineContext context;

    public static SparkSession spark;
    public static SQLContext sqlContext;
    public static JavaSparkContext jsc;

    public static boolean enableHive = false;
    /**
     * An indicator of the initialization status.
     */
    protected boolean initialized = false;

    public static String HUDI_PATH = "file://" + System.getProperty("user.dir") + "/target/";

    public static String TBL_NAME = "";

    public static String PATH = "";

    public static  String CREATE_DDL;

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

    public static void init(){
        PATH = HUDI_PATH + TBL_NAME;
    }
    @BeforeEach
    public synchronized void runBeforeEach() {
        initialized = spark != null;
        if (!initialized) {
            SparkConf sparkConf = conf();
            SparkRDDReadClient.addHoodieSupport(sparkConf);
            SparkSession.Builder builder = SparkSession.builder()
                    .master("local[1]")
                    .appName("hoodie sql test")
                    .withExtensions(new HoodieSparkSessionExtension())
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("hoodie.insert.shuffle.parallelism", "4")
                    .config("hoodie.upsert.shuffle.parallelism", "4")
                    .config("hoodie.delete.shuffle.parallelism", "4")
//                    .config("spark.sql.warehouse.dir", "/Users/hunter/workspace/lmq/hudi-master-debug/hudi-debug/hudi-debug-spark/target/TestSparkSQL")
                    .config("spark.sql.session.timeZone", "UTC")
                    .config(sparkConf);
            if (enableHive) {
                builder.enableHiveSupport();
            }
            spark = builder.getOrCreate();
            sqlContext = spark.sqlContext();
            jsc = new JavaSparkContext(spark.sparkContext());
            context = new HoodieSparkEngineContext(jsc);
        }
    }
}
