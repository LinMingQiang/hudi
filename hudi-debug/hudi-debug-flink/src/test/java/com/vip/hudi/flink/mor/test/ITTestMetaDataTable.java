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

    static {
        SINK_PRO.put("metadata.enabled", "true");
    }
    /**
     * 1,name,1,1,par1
     * 1,,2,2,par1
     * -- 1,name,2,2,par1
     * @throws Exception
     */
    @Test
    public void testWriteA() throws Exception {
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.executeSql("insert into ITTestMetaDataTable(id, age, ts, par)  values('1',2,2,'par1')").await();
        tableEnv.executeSql("insert into ITTestMetaDataTable(id, name, ts, par) values('1','name',1,'par1')").await();
    }

    @org.junit.Test
    public void testRead() throws Exception {
        initEnvAndDefaultMOR(RuntimeExecutionMode.BATCH, TABLE_NAME);
        tableEnv.sqlQuery("select * from " + TABLE_NAME +
                "/*+ OPTIONS(" +
                "'read.streaming.enabled' = 'false'," +
                "'read.tasks'='1'," +
                "'read.start-commit'='0'," +
                "'read.end-commit'='20231013101032156') */"
        ).execute().print();
    }
}
