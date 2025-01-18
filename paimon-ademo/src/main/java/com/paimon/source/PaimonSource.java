package com.paimon.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PaimonSource {
    public static void main(String[] args) throws Exception {
        // 动态获取项目根目录
        String projectRoot = System.getProperty("user.dir");

        // 构造 Paimon 仓库路径
        String paimonWarehouse = projectRoot + "/lakehouse/paimon1";
        Path warehousePath = Paths.get(paimonWarehouse);

        // 确保目录存在
        if (!Files.exists(warehousePath)) {
            Files.createDirectories(warehousePath);
        }

        // 构造检查点路径
        String checkpointPath = projectRoot + "/lakehouse/chk";

        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8082");
        // 设置执行环境
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);
        /** 设置检查点的时间间隔 */
        env.enableCheckpointing(120000);
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                "file://" + checkpointPath, true);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(rocksDBStateBackend);
        // 创建 TableEnvironment
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().getConfiguration().setString("plugin.path", "/Volumes/karekinSSD1/project/paimon/paimon-plugins");


        String cdcDLL = "CREATE TABLE IF NOT EXISTS input (\n" +
                "  `id` bigint,\n" +
                "  `name` String,\n" +
                "  `dt` string,\n" +
                "   PRIMARY KEY ( `id` ) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = '192.168.1.11',\n" +
                "'port' = '3306',\n" +
                "'username' = 'root',\n" +
                "'password' = '666666',\n" +
                "'database-name' = 'test',\n" +
                "'scan.startup.mode' = 'initial',\n" +
                "'table-name' = 'p_source4'\n" +
                ") ";
        tableEnv.executeSql(cdcDLL);

        String catalog = String.format(
                "CREATE CATALOG paimon WITH (\n" +
                        "    'type' = 'paimon',\n" +
                        "    'warehouse' = 'file:%s'\n" +
                        ")", paimonWarehouse.replace("\\", "/"));

        tableEnv.executeSql(catalog);
        tableEnv.executeSql("create database if not exists paimon.test ");
        tableEnv.executeSql("CREATE TABLE if not exists paimon.test.ods_flink (\n" +
                " `id` bigint,\n" +
                " `name` String,\n" +
                " `dt`  string,\n" +
                "  PRIMARY KEY (id,dt) NOT ENFORCED\n" +
                ") partitioned by (dt) with  (\n" +
                "   'changelog-producer' = 'input',\n" +
                //"   'local-merge-buffer-size' = '64mb',\n" +
                "   'bucket' = '1' \n" +
                ") ") ;
        tableEnv.executeSql("insert into paimon.test.ods_flink select id,name,dt from " +
                "default_catalog.default_database.input ").print();
        env.execute("Flink SQL Demo");

    }
}
