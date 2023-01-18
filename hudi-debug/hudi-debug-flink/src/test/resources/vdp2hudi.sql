CREATE TABLE vdp_order_test_mor(
                                   binlog_event_type string,
                                   binlog_create_time bigint,
                                   binlog_update_time bigint,
                                   binlog_is_deleted int,
                                   binlog_schema_name string,
                                   binlog_table_name string,
                                   binlog_position bigint,
                                   order_id INT,
                                   create_time TIMESTAMP(3),
                                   customer_name STRING,
                                   price DECIMAL(10, 5),
                                   product_id INT,
                                   order_status INT,
                                   last_update_time TIMESTAMP(3),
                                   dt STRING,
                                   PRIMARY KEY(order_id) NOT ENFORCED
)
WITH (
  'connector' = 'hudi',
  'path' = 'file:///Users/hunter/workspace/vip/hudi/hudi-examples/hudi-examples-debug/target/vdp_order_test_mor', -- file:///localfilepath/vdp_order_test_mor_rt
  'write.tasks' = '3', -- default is 4 ,required more resource
  'write.bucket_assign.tasks'='11',
  'changelog.enabled'='true',
  'write.task.max.size' = '2048D',
  'write.batch.size'='1D',
  'index.bootstrap.enabled'='true',
  'compaction.delta_commits' = '5',
  'compaction.max_memory' = '1024',
  'table.type' = 'MERGE_ON_READ', -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE
  'compaction.async.enabled'='false',
  'write.precombine'='true',
  'write.precombine.field'='last_update_time'
);
INSERT INTO vdp_order_test_mor
select
    binlog_event_type,
    binlog_create_time,
    binlog_update_time,
    binlog_is_deleted,
    binlog_schema_name,
    binlog_table_name,
    binlog_position,
    order_id,
    create_time,
    customer_name,
    price,
    product_id,
    order_status,
    last_update_time,
    DATE_FORMAT(last_update_time,'yyyyMMdd') as dt
from
    vdp.cdc_hudi_test_168_3306.orders
/*+ OPTIONS('application'='cdc_hudi_test',
 'token'='2f3c8f3d63034f868d0b0a9f6057f01e',
 'scan.startup.mode' = 'earliest-offset' ) */
;
