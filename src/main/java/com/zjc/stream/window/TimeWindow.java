package com.zjc.stream.window;

import com.zjc.stream.wc.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2021/12/20
 * @description :
 */
public class TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\mylocal\\flink-rush\\src\\main\\resources\\sensor.txt");
        
        dataStreamSource
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
                }).keyBy("id")
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
//                .countWindow()
                .timeWindow(Time.seconds(1))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    private static final long serialVersionUID = 2235998503070640967L;
    
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }
                    
                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }
                    
                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }
                    
                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                }).print("累加器");
        
        
        env.execute();
    }
}
