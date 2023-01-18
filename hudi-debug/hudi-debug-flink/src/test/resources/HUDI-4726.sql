
CREATE TABLE bulk_insert_test_mor(
                                     binlog_event_type string,
                                     binlog_create_time bigint,
                                     binlog_update_time bigint,
                                     binlog_is_deleted int,
                                     binlog_schema_name string,
                                     binlog_table_name string,
                                     binlog_position bigint,
                                     order_id INT,
                                     create_time string,
                                     customer_name STRING,
                                     price DECIMAL(10, 5),
                                     product_id INT,
                                     order_status INT,
                                     last_update_time string,
                                     dt STRING,
                                     PRIMARY KEY(order_id) NOT ENFORCED
) PARTITIONED BY (`dt`)
    WITH (
        'connector' = 'hudi',
        'write.operation'='bulk_insert',
        'path' = 'file:///Users/hunter/workspace/vip/hudi/hudi-examples/hudi-examples-debug/target/bulk_insert_test_mor',
        'index.type' = 'BUCKET',
        'hoodie.bucket.index.num.buckets' = '10',
        'changelog.enabled'='true',
        'compaction.delta_commits' = '1',
        'compaction.max_memory' = '1024',
        'table.type' = 'MERGE_ON_READ', -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE
        'compaction.async.enabled'='true',
        'write.precombine'='true',
        'write.precombine.field'='last_update_time'
);
