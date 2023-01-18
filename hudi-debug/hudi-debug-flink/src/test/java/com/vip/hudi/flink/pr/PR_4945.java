package com.vip.hudi.flink.pr;

import com.vip.hudi.flink.utils.TestUtil;
import org.apache.flink.api.common.RuntimeExecutionMode;

import java.util.concurrent.ExecutionException;

public class PR_4945 extends TestUtil {
    // https://github.com/apache/hudi/pull/6845
    // batch支持clean
    String tblName = "PR_4945";


    @org.junit.Test
    public void testWrite() throws ExecutionException, InterruptedException{
        initEnv(RuntimeExecutionMode.BATCH);
        tableEnv.executeSql(DATAGEN_SRC_TBL(false));
        tableEnv.executeSql(HUDI_MOR_TBL(tblName));
        String insertSql = "insert into HUDI_MOR_TBL " +
                "/*+ OPTIONS(" +
                "'clean.retain_commits'='1'" +
                ") */" +
                " select " +
                "id," +
                "name," +
                "age," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd HH:mm:ss') as rowtime," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd') as dt" +
                " from DATAGEN_SRC_TBL";

        tableEnv.executeSql(insertSql).await();
    }


    @org.junit.Test
    public void testStreamingWrite() throws ExecutionException, InterruptedException{
        initEnv(RuntimeExecutionMode.STREAMING);
        tableEnv.executeSql(DATAGEN_SRC_TBL(true));
        tableEnv.executeSql(HUDI_MOR_TBL(tblName));
        String insertSql = "insert into HUDI_MOR_TBL " +
                "/*+ OPTIONS(" +
                "'clean.retain_commits'='1'" +
                ") */" +
                " select " +
                "id," +
                "name," +
                "age," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd HH:mm:ss') as rowtime," +
                "DATE_FORMAT(rowtime,'yyyy-MM-dd') as dt" +
                " from DATAGEN_SRC_TBL";

        tableEnv.executeSql(insertSql).await();
    }
}
