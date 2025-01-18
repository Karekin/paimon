package com.paimon.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PaimonQuery {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8081");
        // 设置执行环境
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);
        /** 设置检查点的时间间隔 */
        //env.enableCheckpointing(3000);
        // 创建 TableEnvironment
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String catalog = "CREATE CATALOG paimon WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = 'file:/D:/lakehouse/paimon'" +
                "    )";

        tableEnv.executeSql(catalog);
        tableEnv.executeSql("create database if not exists paimon.test ");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.ods_flink (\n" +
                " `id` bigint,\n" +
                " `name` String,\n" +
                " `dt`  string,\n" +
                "  PRIMARY KEY (id,dt) NOT ENFORCED\n" +
                ") partitioned by (dt) with  (\n" +
                "   'changelog-producer' = 'input',\n" +
                "   'bucket' = '2' \n" +
                ") ") ;
        tableEnv.executeSql("select id,name,dt from paimon.test.ods_flink ").print();
        env.execute("Flink SQL Demo");

    }
}
