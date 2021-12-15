package com.zjc.batch.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2021/12/13
 * @description : 批处理wc
 */
public class WordCount {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
//        String inputPath = "E:\\mylocal\\flink-rush\\src\\main\\resources\\hello.txt";
//        DataSet<String> inputDataSet = env.readTextFile(inputPath);
        List<String> elements = Arrays.asList("hello world good hello job flink hello job hello flink spark");
        DataSource<String> inputDataSet = env.fromCollection(elements);
        //对数据集进行处理
        AggregateOperator<Tuple2<String, Integer>> operator = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0) //按照第一个位置的word分组
                .sum(1);//将第二个位置上的数据求和
        operator.print();
    }
    
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //按空格分词
            String[] words = s.split(" ");
            //遍历所有word包成二元组输出
            Arrays.stream(words).forEach(word -> {
                collector.collect(new Tuple2<>(word, 1));
            });
        }
    }
   
}

