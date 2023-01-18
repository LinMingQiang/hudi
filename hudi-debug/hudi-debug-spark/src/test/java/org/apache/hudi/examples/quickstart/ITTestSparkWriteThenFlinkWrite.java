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

/**
 * spark先写到hoodie，然后同步表A （ro）。
 * --
 * 然后flink再写入，同步表A （ro）。
 * 1：如果flink写的字段顺序和spark的顺序不一致可以吗
 */
public class ITTestSparkWriteThenFlinkWrite implements SparkProvider {
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

    @Test
    public void testSparkWriteThenFlinkWrite() {
//        spark.sql("create table test.ITTestSparkWriteThenFlinkWrite(\n" +
//                        "  id string not null," +
//                        "  name string," +
//                        " age bigint," +
//                        " ts bigint," +
//                        "  par string" +
//                        ") using hudi\n" +
//                        " tblproperties (\n" +
//                        "  type = 'mor'\n" +
//                        "  ,primaryKey = 'id'\n" +
//                        "  ,hoodie.index.type = 'BUCKET'\n" +
//                        "  ,hoodie.storage.layout.type = 'BUCKET'\n" +
//                        "  ,hoodie.bucket.index.num.buckets = '1'\n" +
//                        "  ,preCombineField = 'ts'" +
//                        "  ,hoodie.datasource.hive_sync.enable = 'true'" +
//                        "  ,hoodie.datasource.hive_sync.table.strategy='ro'" +
//                        "  ,hoodie.datasource.hive_sync.table='ITTestSparkWriteThenFlinkWrite'" +
//                        "  ,hoodie.datasource.hive_sync.database='test'" +
//                        "  ,hoodie.storage.layout.partitioner.class='org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner'" +
//                        "  ,hoodie.datasource.write.payload.class='org.apache.hudi.common.model.PartialUpdateAvroPayload'" +
////                "  ,hoodie.base.path='file:///Users/hunter/workspace/lmq/hudi-master-debug/hudi-debug/hudi-debug-flink/target/ITTestSparkWriteThenFlinkWrite'" +
//                        ") \n " +
//                        " partitioned by (par) \n"
//                        + " location '/Users/hunter/workspace/lmq/hudi-master-debug/hudi-debug/hudi-debug-flink/target/ITTestSparkWriteThenFlinkWrite'"
//        );
//        spark.table("test.ITTestSparkWriteThenFlinkWrite").printSchema();

        spark.sql("merge into test.ITTestSparkWriteThenFlinkWrite as target " +
                " using (select 'id2' as id, 'name' as name, 1 as age, 1 as ts, 'par1' as par ) as source " +
                " ON target.`id` = source.`id` " +
                "WHEN MATCHED THEN UPDATE SET * " +
                "WHEN NOT MATCHED THEN INSERT * ");
    }
}