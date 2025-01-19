package com.paimon.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 *
 */

public class ApiHelloWord {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //设置WebUI绑定的本地端口
        conf.setString(RestOptions.BIND_PORT,"8082");
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf);
        env.setParallelism(2);

        /** 读取socket数据 */
        DataStreamSource<String> fileStream =   env.socketTextStream("127.0.0.1",9999);
        /** 将数据转成小写 */
        SingleOutputStreamOperator<String> mapStream = fileStream.map(String :: toUpperCase);
        /** 按照空格切分字符串*/
        SingleOutputStreamOperator<Tuple2<String,Integer>> flatMapStream = mapStream.flatMap(new Split());
        /** 分组聚合*/
        KeyedStream<Tuple2<String,Integer>,String> keyStream = flatMapStream.keyBy(value -> value.f0);
        /** 聚合*/
        SingleOutputStreamOperator<Tuple2<String,Integer>> sumStream = keyStream.sum(1);
        sumStream.print();
        /** 执行任务 */
        env.execute("WordCount");
    }

    public static class Split implements FlatMapFunction<String, Tuple2<String,Integer>> {

        /**
         * 按照空格切分数据
         * @param element
         * @param collector
         * @throws Exception
         */
        @Override
        public void flatMap(String element, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String [] eles = element.split(" ");
            for(String chr : eles){
                collector.collect(new Tuple2<>(chr,1));
            }
        }
    }
}


