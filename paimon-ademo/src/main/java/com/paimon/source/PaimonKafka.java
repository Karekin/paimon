package com.paimon.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PaimonKafka {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8082");
        // 设置执行环境
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);
        /** 设置检查点的时间间隔 */
        env.enableCheckpointing(10000);
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                "file:///D:/lakehouse/chk", true);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(rocksDBStateBackend);
        // 创建 TableEnvironment
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String cdcDLL = "CREATE TABLE IF NOT EXISTS input (\n" +
                "  `id` bigint,\n" +
                "  `name` String,\n" +
                "  `dt` string,\n" +
                "   PRIMARY KEY ( `id` ) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = 'mj_mysql',\n" +
                "'port' = '13306',\n" +
                "'username' = 'root',\n" +
                "'password' = 'nn19910313_',\n" +
                "'database-name' = 'test',\n" +
                "'scan.startup.mode' = 'initial',\n" +
                "'table-name' = 'p_source'\n" +
                ") ";
        tableEnv.executeSql(cdcDLL);

        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                " `id` bigint,\n" +
                "  `name` String,\n" +
                "  `dt` string \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'paimon_test',\n" +
                "  'properties.bootstrap.servers' = '192.168.110.45:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'value.format' = 'debezium-json'\n" +
                ") ") ;
        tableEnv.executeSql("insert into KafkaTable select id,name,dt from input ").print();
        env.execute("Flink SQL Demo");

    }
}
