package com.vip.hudi.flink.mor.test;

import com.vip.hudi.flink.utils.TestSQLManager;
import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ITTestMetaDataTable extends TestUtil {
    public static String TABLE_NAME = "ITTestMetaDataTable";

    public static String HUDI_DDL ;
    static {
        Map<String, String> pro = new HashMap<>();
        pro.put("metadata.enabled", "true");
        HUDI_DDL = TestSQLManager.HUDI_DDL_MOR_WITH_PRO(
                TABLE_NAME,
                FILE_HUDI_PATH + TABLE_NAME,
                pro);
        init(RuntimeExecutionMode.BATCH);
        tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.upsert-materialize", "none");
        tableEnv.executeSql(HUDI_DDL);
    }
    /**
     * 1,name,1,1,par1
     * 1,,2,2,par1
     * -- 1,name,2,2,par1
     * @throws Exception
     */
    @Test
    public void testWriteA() throws Exception {
        tableEnv.executeSql("insert into ITTestMetaDataTable(id, age, ts, par)  values('1',2,2,'par1')").await();
        tableEnv.executeSql("insert into ITTestMetaDataTable(id, name, ts, par) values('1','name',1,'par1')").await();
    }

    @org.junit.Test
    public void testRead() throws Exception {
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from " + TABLE_NAME +
                        "/*+ OPTIONS(" +
                        "'read.streaming.enabled' = 'false'," +
                        "'read.tasks'='1'," +
                        "'read.start-commit'='0'," +
                        "'read.end-commit'='20231013101032156') */"
                ), Row.class)
                .print();
        env.execute();
    }
}
