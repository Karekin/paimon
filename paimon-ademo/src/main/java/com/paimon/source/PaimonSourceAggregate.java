package com.paimon.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PaimonSourceAggregate {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8081");
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String catalog = "CREATE CATALOG paimon WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = 'file:/D:/lakehouse/'" +
                "    )";

        tableEnv.executeSql(catalog);
        tableEnv.executeSql("create database if not exists paimon.test ");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS paimon.test.dws_tsgz_event_dest_ip_count_day (\n" +
                "dcd_guid varchar(255),\n" +
                "site varchar(255),\n" +
                "src_ip varchar(255),\n" +
                "a int,\n" +
                "src_ip_sales BIGINT,\n" +
                "dest_ip varchar(255),\n" +
                "b int,\n" +
                "dest_ip_sales BIGINT,\n" +
                "PRIMARY KEY (dcd_guid,site) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'paimon', \n" +
                "'bucket' = '3',\n" +
                "'changelog-producer' = 'full-compaction',\n" +
                "'merge-engine' = 'partial-update',\n" +
               // "'partial-update.remove-record-on-delete' = 'true',\n" +
                "'write-buffer-spillable' = 'true',\n" +
                "'fields.a.sequence-group' = 'src_ip_sales',\n" +
                "'fields.src_ip_sales.aggregate-function' = 'sum',\n" +
                "'fields.b.sequence-group' = 'dest_ip_sales',\n" +
                "'fields.dest_ip_sales.aggregate-function' = 'sum'\n" +
                ")") ;

        //tableEnv.executeSql("insert into paimon.test.dws_tsgz_event_dest_ip_count_day values('111','111','22',1,2,'111',1,3) ").print();
        //  DATE_FORMAT(CAST(PROCTIME() AS TIMESTAMP(3)), 'yyyy-MM-dd HH:mm:ss') as formatted_proc_time,
        //tableEnv.executeSql("select TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(CAST(PROCTIME() AS TIMESTAMP(3)), 'Seconds'),3) ").print();
        //tableEnv.executeSql("select CAST(PROCTIME() AS TIMESTAMP(3))").print();
        //tableEnv.executeSql("select TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(PROCTIME()),3)").print();
        tableEnv.executeSql("select TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP( DATE_FORMAT(CAST(PROCTIME() AS TIMESTAMP(3)), 'yyyy-MM-dd HH:mm:ss')),3)").print();
        env.execute("Flink SQL Demo");

    }
}
