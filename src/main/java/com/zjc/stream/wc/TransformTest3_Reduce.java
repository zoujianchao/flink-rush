package com.zjc.stream.wc;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2021/12/15
 * @description :
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setParallelism(1);
        
        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\mylocal\\flink-rush\\src\\main\\resources\\sensor.txt");
        
        dataStreamSource
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
                }).keyBy(SensorReading::getId)
                .reduce((curSensor, newSensor) ->
                        new SensorReading(curSensor.getId(), newSensor.getTimestamp(), Math.max(curSensor.getTemperature(), newSensor.getTemperature()))
                ).print("result");
        env.execute();
    }
}
