package com.zjc.stream.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2021/12/13
 * @description : 有状态的流处理wc
 */
public class WordCount {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(3);
        //从文件中读取数据
        String inputPath = "E:\\mylocal\\flink-rush\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputDataStream = env.readTextFile(inputPath);
        //对数据集进行处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = inputDataStream.flatMap(new com.zjc.batch.wc.WordCount.MyFlatMapper())
                .keyBy(0) //按照第一个位置的word分组
                .sum(1);//将第二个位置上的数据求和
        operator.print();
        
        //执行任务
        env.execute();
    }
    
}
