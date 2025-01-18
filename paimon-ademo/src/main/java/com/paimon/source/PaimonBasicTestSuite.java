package com.paimon.source;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 码界探索
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * @desc: PaimonBasicTestSuite
 */
public class PaimonBasicTestSuite {
    public static void main(String[] args)  throws Exception{
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8082");
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setMaxParallelism(2);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(2);
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000L);
         StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String cdcDLL = "CREATE TABLE IF NOT EXISTS input (\n" +
                "  `id` bigint,\n" +
                "  `name` String,\n" +
                "  `dt` string,\n" +
                "   PRIMARY KEY ( `id` ) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'datagen'\n" +
                ") ";
        tableEnv.executeSql(cdcDLL);
        String catalog = "CREATE CATALOG paimon WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = 'file:/D:/lakehouse/paimon1'" +
                "    )";

        tableEnv.executeSql(catalog);
        tableEnv.executeSql("create database if not exists paimon.test ");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.ods_flink (\n" +
                " `id` bigint,\n" +
                " `name` String,\n" +
                " `dt`  string,\n" +
                "  PRIMARY KEY (id,dt) NOT ENFORCED\n" +
                ") partitioned by (dt) with  (\n" +
                "'merge-engine' = 'deduplicate',\n" +
                "'bucket' = '1',\n" +
                "'changelog-producer'='none'\n" +
                ") ") ;
        tableEnv.executeSql("insert into paimon.test.ods_flink select id,name,dt from " +
                "default_catalog.default_database.input ").print();
        env.execute("Flink SQL Demo");

    }
}
