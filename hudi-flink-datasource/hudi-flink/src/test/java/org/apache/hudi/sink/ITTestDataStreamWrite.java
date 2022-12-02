/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.transform.ChainedTransformer;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.table.action.commit.FlinkWriteHelper;
import org.apache.hudi.table.catalog.HoodieCatalog;
import org.apache.hudi.table.catalog.TableOptionProperties;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.TestUtils;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.apache.parquet.Strings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DATABASE;

/**
 * Integration test for Flink Hoodie stream sink.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestDataStreamWrite extends TestLogger {

  private static final Map<String, List<String>> EXPECTED = new HashMap<>();
  private static final Map<String, List<String>> EXPECTED_TRANSFORMER = new HashMap<>();
  private static final Map<String, List<String>> EXPECTED_CHAINED_TRANSFORMER = new HashMap<>();

  static {
    EXPECTED.put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
    EXPECTED.put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
    EXPECTED.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
    EXPECTED.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));

    EXPECTED_TRANSFORMER.put("par1", Arrays.asList("id1,par1,id1,Danny,24,1000,par1", "id2,par1,id2,Stephen,34,2000,par1"));
    EXPECTED_TRANSFORMER.put("par2", Arrays.asList("id3,par2,id3,Julian,54,3000,par2", "id4,par2,id4,Fabian,32,4000,par2"));
    EXPECTED_TRANSFORMER.put("par3", Arrays.asList("id5,par3,id5,Sophia,19,5000,par3", "id6,par3,id6,Emma,21,6000,par3"));
    EXPECTED_TRANSFORMER.put("par4", Arrays.asList("id7,par4,id7,Bob,45,7000,par4", "id8,par4,id8,Han,57,8000,par4"));

    EXPECTED_CHAINED_TRANSFORMER.put("par1", Arrays.asList("id1,par1,id1,Danny,25,1000,par1", "id2,par1,id2,Stephen,35,2000,par1"));
    EXPECTED_CHAINED_TRANSFORMER.put("par2", Arrays.asList("id3,par2,id3,Julian,55,3000,par2", "id4,par2,id4,Fabian,33,4000,par2"));
    EXPECTED_CHAINED_TRANSFORMER.put("par3", Arrays.asList("id5,par3,id5,Sophia,20,5000,par3", "id6,par3,id6,Emma,22,6000,par3"));
    EXPECTED_CHAINED_TRANSFORMER.put("par4", Arrays.asList("id7,par4,id7,Bob,46,7000,par4", "id8,par4,id8,Han,58,8000,par4"));
  }

  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(strings = {"BUCKET", "FLINK_STATE"})
  public void testWriteCopyOnWrite(String indexType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.setString(FlinkOptions.INDEX_TYPE, indexType);
    conf.setInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 1);
    conf.setString(FlinkOptions.INDEX_KEY_FIELD, "id");
    conf.setBoolean(FlinkOptions.PRE_COMBINE, true);

    testWriteToHoodie(conf, "cow_write", 2, EXPECTED);
  }

  @Test
  public void testWriteCopyOnWriteWithTransformer() throws Exception {
    Transformer transformer = (ds) -> ds.map((rowdata) -> {
      if (rowdata instanceof GenericRowData) {
        GenericRowData genericRD = (GenericRowData) rowdata;
        //update age field to age + 1
        genericRD.setField(2, genericRD.getInt(2) + 1);
        return genericRD;
      } else {
        throw new RuntimeException("Unrecognized row type information: " + rowdata.getClass().getSimpleName());
      }
    });

    testWriteToHoodie(transformer, "cow_write_with_transformer", EXPECTED_TRANSFORMER);
  }

  @Test
  public void testWriteCopyOnWriteWithChainedTransformer() throws Exception {
    Transformer t1 = (ds) -> ds.map(rowData -> {
      if (rowData instanceof GenericRowData) {
        GenericRowData genericRD = (GenericRowData) rowData;
        //update age field to age + 1
        genericRD.setField(2, genericRD.getInt(2) + 1);
        return genericRD;
      } else {
        throw new RuntimeException("Unrecognized row type : " + rowData.getClass().getSimpleName());
      }
    });

    ChainedTransformer chainedTransformer = new ChainedTransformer(Arrays.asList(t1, t1));

    testWriteToHoodie(chainedTransformer, "cow_write_with_chained_transformer", EXPECTED_CHAINED_TRANSFORMER);
  }

  @ParameterizedTest
  @ValueSource(strings = {"BUCKET", "FLINK_STATE"})
  public void testWriteMergeOnReadWithCompaction(String indexType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.setString(FlinkOptions.INDEX_TYPE, indexType);
    conf.setInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, 4);
    conf.setString(FlinkOptions.INDEX_KEY_FIELD, "id");
    conf.setInteger(FlinkOptions.COMPACTION_DELTA_COMMITS, 1);
    conf.setString(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());

    testWriteToHoodie(conf, "mor_write_with_compact", 1, EXPECTED);
  }

  @Test
  public void testWriteCopyOnWriteWithClustering() throws Exception {
    testWriteCopyOnWriteWithClustering(false);
  }

  @Test
  public void testWriteCopyOnWriteWithSortClustering() throws Exception {
    testWriteCopyOnWriteWithClustering(true);
  }

  private void testWriteCopyOnWriteWithClustering(boolean sortClusteringEnabled) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.setBoolean(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, true);
    conf.setInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS, 1);
    conf.setString(FlinkOptions.OPERATION, "insert");
    if (sortClusteringEnabled) {
      conf.setString(FlinkOptions.CLUSTERING_SORT_COLUMNS, "uuid");
    }

    testWriteToHoodieWithCluster(conf, "cow_write_with_cluster", 1, EXPECTED);
  }

  private void testWriteToHoodie(
      Transformer transformer,
      String jobName,
      Map<String, List<String>> expected) throws Exception {
    testWriteToHoodie(TestConfigurations.getDefaultConf(tempFile.toURI().toString()),
        Option.of(transformer), jobName, 2, expected);
  }

  private void testWriteToHoodie(
      Configuration conf,
      String jobName,
      int checkpoints,
      Map<String, List<String>> expected) throws Exception {
    testWriteToHoodie(conf, Option.empty(), jobName, checkpoints, expected);
  }

  private void testWriteToHoodie(
      Configuration conf,
      Option<Transformer> transformer,
      String jobName,
      int checkpoints,
      Map<String, List<String>> expected) throws Exception {

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    boolean isMor = conf.getString(FlinkOptions.TABLE_TYPE).equals(HoodieTableType.MERGE_ON_READ.name());

    DataStream<RowData> dataStream;
    if (isMor) {
      TextInputFormat format = new TextInputFormat(new Path(sourcePath));
      format.setFilesFilter(FilePathFilter.createDefaultFilter());
      TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
      format.setCharsetName("UTF-8");

      dataStream = execEnv
          // use PROCESS_CONTINUOUSLY mode to trigger checkpoint
          .readFile(format, sourcePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, typeInfo)
          .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
          .setParallelism(1);
    } else {
      dataStream = execEnv
          // use continuous file source to trigger checkpoint
          .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), checkpoints))
          .name("continuous_file_source")
          .setParallelism(1)
          .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
          .setParallelism(4);
    }

    if (transformer.isPresent()) {
      dataStream = transformer.get().apply(dataStream);
    }

    OptionsInference.setupSinkTasks(conf, execEnv.getParallelism());
    DataStream<HoodieRecord> hoodieRecordDataStream = Pipelines.bootstrap(conf, rowType, dataStream);
    DataStream<Object> pipeline = Pipelines.hoodieStreamWrite(conf, hoodieRecordDataStream);
    execEnv.addOperator(pipeline.getTransformation());

    if (isMor) {
      Pipelines.compact(conf, pipeline);
    }

    execute(execEnv, isMor, jobName);
    TestData.checkWrittenDataCOW(tempFile, expected);
  }

  private void testWriteToHoodieWithCluster(
      Configuration conf,
      String jobName,
      int checkpoints,
      Map<String, List<String>> expected) throws Exception {

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    DataStream<RowData> dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), checkpoints))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(4);

    OptionsInference.setupSinkTasks(conf, execEnv.getParallelism());
    DataStream<Object> pipeline = Pipelines.append(conf, rowType, dataStream, true);
    execEnv.addOperator(pipeline.getTransformation());

    Pipelines.cluster(conf, rowType, pipeline);
    execute(execEnv, false, jobName);

    TestData.checkWrittenDataCOW(tempFile, expected);
  }

  public void execute(StreamExecutionEnvironment execEnv, boolean isMor, String jobName) throws Exception {
    if (isMor) {
      JobClient client = execEnv.executeAsync(jobName);
      if (client.getJobStatus().get() != JobStatus.FAILED) {
        try {
          TimeUnit.SECONDS.sleep(20); // wait long enough for the compaction to finish
          client.cancel();
        } catch (Throwable var1) {
          // ignored
        }
      }
    } else {
      // wait for the streaming job to finish
      execEnv.execute(jobName);
    }
  }

  @Test
  public void testHoodiePipelineBuilderSource() throws Exception {
    //create a StreamExecutionEnvironment instance.
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(1);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.setString(FlinkOptions.TABLE_NAME, "t1");
    conf.setString(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");

    // write 3 batches of data set
    TestData.writeData(TestData.dataSetInsert(1, 2), conf);
    TestData.writeData(TestData.dataSetInsert(3, 4), conf);
    TestData.writeData(TestData.dataSetInsert(5, 6), conf);

    String latestCommit = TestUtils.getLastCompleteInstant(tempFile.toURI().toString());

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.READ_START_COMMIT.key(), latestCommit);

    //read a hoodie table use low-level source api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("test_source")
        .column("uuid string not null")
        .column("name string")
        .column("age int")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);
    DataStream<RowData> rowDataDataStream = builder.source(execEnv);
    List<RowData> result = new ArrayList<>();
    rowDataDataStream.executeAndCollect().forEachRemaining(result::add);
    TimeUnit.SECONDS.sleep(2);//sleep 2 second for collect data
    TestData.assertRowDataEquals(result, TestData.dataSetInsert(5, 6));
  }

  @Test
  public void testHoodiePipelineBuilderSink() throws Exception {
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    Map<String, String> options = new HashMap<>();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH.key(), Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    Configuration conf = Configuration.fromMap(options);
    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    TextInputFormat format = new TextInputFormat(new Path(sourcePath));
    format.setFilesFilter(FilePathFilter.createDefaultFilter());
    format.setCharsetName("UTF-8");

    DataStream dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), 2))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(4);

    //sink to hoodie table use low-level sink api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("name string")
        .column("age int")
        .column("`ts` timestamp(3)")
        .column("`partition` string")
        .pk("uuid")
        .partition("partition")
        .options(options);

    builder.sink(dataStream, false);

    execute(execEnv, false, "Api_Sink_Test");
    TestData.checkWrittenDataCOW(tempFile, EXPECTED);
  }

  @Test
  public void testHoodiePipelineBuilderSourceWithSchemaSet() throws Exception {
    //create a StreamExecutionEnvironment instance.
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(1);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // create table dir
    final String dbName = DEFAULT_DATABASE.defaultValue();
    final String tableName = "t1";
    File testTable = new File(tempFile, dbName + Path.SEPARATOR + tableName);
    testTable.mkdir();

    Configuration conf = TestConfigurations.getDefaultConf(testTable.toURI().toString());
    conf.setString(FlinkOptions.TABLE_NAME, tableName);
    conf.setString(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");

    // write 3 batches of data set
    TestData.writeData(TestData.dataSetInsert(1, 2), conf);
    TestData.writeData(TestData.dataSetInsert(3, 4), conf);
    TestData.writeData(TestData.dataSetInsert(5, 6), conf);

    String latestCommit = TestUtils.getLastCompleteInstant(testTable.toURI().toString());

    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), testTable.toURI().toString());
    options.put(FlinkOptions.READ_START_COMMIT.key(), latestCommit);

    // create hoodie catalog, in order to get the table schema
    Configuration catalogConf = new Configuration();
    catalogConf.setString(CATALOG_PATH.key(), tempFile.toURI().toString());
    catalogConf.setString(DEFAULT_DATABASE.key(), DEFAULT_DATABASE.defaultValue());
    HoodieCatalog catalog = new HoodieCatalog("hudi", catalogConf);
    catalog.open();
    // get hoodieTable
    ObjectPath tablePath = new ObjectPath(dbName, tableName);
    TableOptionProperties.createProperties(testTable.toURI().toString(), HadoopConfigurations.getHadoopConf(catalogConf), options);
    CatalogBaseTable hoodieTable = catalog.getTable(tablePath);

    //read a hoodie table use low-level source api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("test_source")
        .schema(hoodieTable.getUnresolvedSchema())
        .pk("uuid")
        .partition("partition")
        .options(options);
    DataStream<RowData> rowDataDataStream = builder.source(execEnv);
    List<RowData> result = new ArrayList<>();
    rowDataDataStream.executeAndCollect().forEachRemaining(result::add);
    TimeUnit.SECONDS.sleep(2);//sleep 2 second for collect data
    TestData.assertRowDataEquals(result, TestData.dataSetInsert(5, 6));
  }

  @Test
  public void testHoodiePipelineBuilderSinkWithSchemaSet() throws Exception {
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    Map<String, String> options = new HashMap<>();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH.key(), Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("test_read_schema.avsc")).toString());
    Configuration conf = Configuration.fromMap(options);
    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();

    TextInputFormat format = new TextInputFormat(new Path(sourcePath));
    format.setFilesFilter(FilePathFilter.createDefaultFilter());
    format.setCharsetName("UTF-8");

    DataStream dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), 2))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(4);

    Schema schema =
        Schema.newBuilder()
            .column("uuid", DataTypes.STRING().notNull())
            .column("name", DataTypes.STRING())
            .column("age", DataTypes.INT())
            .column("ts", DataTypes.TIMESTAMP(3))
            .column("partition", DataTypes.STRING())
            .primaryKey("uuid")
            .build();

    //sink to hoodie table use low-level sink api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("test_sink")
        .schema(schema)
        .partition("partition")
        .options(options);

    builder.sink(dataStream, false);

    execute(execEnv, false, "Api_Sink_Test");
    TestData.checkWrittenDataCOW(tempFile, EXPECTED);
  }

  @Test
  public void testMultiplePartialUpdate() throws Exception {
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    Map<String, String> options = new HashMap<>();
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(4);
    // set up checkpoint interval
    execEnv.enableCheckpointing(4000, CheckpointingMode.EXACTLY_ONCE);
    execEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    options.put(FlinkOptions.PATH.key(), tempFile.toURI().toString());
    options.put(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH.key(), Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("test_read_schema_partial_update.avsc")).toString());
    Configuration conf = Configuration.fromMap(options);
    // Read from file source
    RowType rowType =
        (RowType) AvroSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    JsonRowDataDeserializationSchema deserializationSchema = new JsonRowDataDeserializationSchema(
        rowType,
        InternalTypeInfo.of(rowType),
        false,
        true,
        TimestampFormat.ISO_8601
    );
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source_partial_update.data")).toString();

    TextInputFormat format = new TextInputFormat(new Path(sourcePath));
    format.setFilesFilter(FilePathFilter.createDefaultFilter());
    format.setCharsetName("UTF-8");

    DataStream dataStream = execEnv
        // use continuous file source to trigger checkpoint
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), 1))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(record -> deserializationSchema.deserialize(record.getBytes(StandardCharsets.UTF_8)))
        .setParallelism(4);

    options.put(FlinkOptions.PATH.key(), tempFile.getAbsolutePath());
    options.put(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "4");
    options.put(FlinkOptions.INDEX_KEY_FIELD.key(), "uuid");
    options.put(FlinkOptions.HIVE_STYLE_PARTITIONING.key(), "false");
    options.put(FlinkOptions.PAYLOAD_CLASS_NAME.key(), PartialUpdateAvroPayload.class.getName());
    options.put(FlinkOptions.PRE_COMBINE.key(), "true");
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "_ts1:fa|_ts2:fb");

    //sink to hoodie table use low-level sink api.
    HoodiePipeline.Builder builder = HoodiePipeline.builder("test_sink")
        .column("uuid string not null")
        .column("fa string") //部分更新列
        .column("_ts1 bigint")
        .column("fb string") //部分更新列
        //.column("fc string") //部分更新列//部分更新列
        .column("_ts2 bigint")
        //.column("fd string") //部分更新列fb 对应的orderingField，必须要设置
        .pk("uuid")
        .options(options);


    builder.sink(dataStream, false);
    execute(execEnv, true, "Api_Sink_Test");
    Map<String, List<String>> expected = new HashMap<>();

    expected.put("", Arrays.asList("id1,7,7,7,7", "id2,8,8,8,8", "id3,6,6,6,6"));

    TestData.checkWrittenFullDataFunction(tempFile, expected, genericRecord -> {
      List<String> fields = new ArrayList<>();
      fields.add(genericRecord.get("_hoodie_record_key").toString());
      fields.add(genericRecord.get("fa").toString());
      fields.add(genericRecord.get("_ts1").toString());
      fields.add(genericRecord.get("fb").toString());
      fields.add(genericRecord.get("_ts2").toString());
      return Strings.join(fields, ",");
    });
  }

  @Test
  void deduplicateRecords() throws IOException {
    final String SCHEMA = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
        + "    {\"name\": \"fa\", \"type\": [\"null\", \"string\"]},\n"
        + "    {\"name\": \"fb\", \"type\": [\"null\", \"string\"]},\n"
        + "    {\"name\": \"_ts\", \"type\": [\"null\", \"long\"]}\n"
        + "  ]\n"
        + "}";

    String preCombineFields = "_ts";
    List<HoodieAvroRecord> records = new ArrayList<>();
    Schema avroSchema = new Schema.Parser().parse(SCHEMA);
    for (int i = 1; i <= 100; i++) {
      long ts = System.currentTimeMillis();
      GenericRecord row1 = new GenericData.Record(avroSchema);
      row1.put("id", "jack");
      row1.put("fa", i + "");
      row1.put("_ts", ts);
      Comparable<?> orderingVal1 = (Comparable<?>) HoodieAvroUtils.getNestedFieldVal(row1,
          preCombineFields, false, false);
      records.add(new HoodieAvroRecord(new HoodieKey("1", "default"),
          new PartialUpdateAvroPayload(row1, orderingVal1), HoodieOperation.INSERT));
      ts = System.currentTimeMillis();
      GenericRecord row2 = new GenericData.Record(avroSchema);
      row2.put("id", "jack");
      row2.put("fb", i + "");
      row2.put("_ts", ts);
      Comparable<?> orderingVal2 = (Comparable<?>) HoodieAvroUtils.getNestedFieldVal(row2,
          preCombineFields, false, false);
      records.add(new HoodieAvroRecord(new HoodieKey("1", "default"),
          new PartialUpdateAvroPayload(row2, orderingVal2), HoodieOperation.INSERT));
    }

    List<HoodieRecord> deduplicateRecords = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1, avroSchema.toString());
    GenericRecord record = HoodieAvroUtils.bytesToAvro(((PartialUpdateAvroPayload) deduplicateRecords.get(0).getData()).recordBytes, avroSchema);
    assertEquals(deduplicateRecords.size(), 1);
    assertEquals(record.get(1).toString(), "100");
    assertEquals(record.get(1), record.get(2));
  }

  @Test
  void deduplicateRecordsWithMultipleOrderingFields() throws IOException {
    String schema = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
        + "    {\"name\": \"fa\", \"type\": [\"null\", \"string\"]},\n"
        + "    {\"name\": \"_ts1\", \"type\": [\"null\", \"long\"]},\n"
        + "    {\"name\": \"fb\", \"type\": [\"null\", \"string\"]},\n"
        + "    {\"name\": \"_ts2\", \"type\": [\"null\", \"long\"]}\n"
        + "  ]\n"
        + "}";
    String preCombineFields = "_ts1:fa|_ts2:fb";
    List<HoodieAvroRecord> records = new ArrayList<>();
    Schema avroSchema = new Schema.Parser().parse(schema);
    for (int i = 1; i <= 1000; i++) {
      long ts = System.currentTimeMillis();
      GenericRecord row1 = new GenericData.Record(avroSchema);
      row1.put("id", "jack");
      row1.put("fa", i + "");
      row1.put("_ts1", ts);
      records.add(new HoodieAvroRecord(new HoodieKey("1", "default"),
          new PartialUpdateAvroPayload(row1, preCombineFields), HoodieOperation.INSERT));
      ts = System.currentTimeMillis();
      GenericRecord row2 = new GenericData.Record(avroSchema);
      row2.put("id", "jack");
      row2.put("fb", i + "");
      row2.put("_ts2", ts);
      records.add(new HoodieAvroRecord(new HoodieKey("1", "default"),
          new PartialUpdateAvroPayload(row2, preCombineFields), HoodieOperation.INSERT));
    }
    records = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1, schema);
    GenericRecord record = HoodieAvroUtils.bytesToAvro(((PartialUpdateAvroPayload) records.get(0).getData()).recordBytes, avroSchema);
    assertEquals(record.get(1).toString(), "1000");
    assertEquals(record.get(1), record.get(3));
  }
}
