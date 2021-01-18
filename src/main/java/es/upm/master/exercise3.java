package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;
import java.util.Iterator;

public class exercise3 {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // Sets up the execution environment

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String input = "";
        String output1 = "";
        String output2 = "";
        int segment = 0;

        // Gets all the user variables

        try {
            input = params.get("input");
            output1 = params.get("output1");
            output2 = params.get("output2");
            segment = Integer.parseInt(params.get("segment"));
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Input file, output folder paths and segment must be provided as argument to this program. Aborting...");
            return;
        }

        final int FinalSegment = segment;
        DataStream<String> text = env.readTextFile(input);

        // Filters Out all observations in different segments than requested
        // Maps the input from string to correct type

        SingleOutputStreamOperator<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = text
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        String[] fieldArray = value.split(",");
                        int seg = Integer.parseInt(fieldArray[6]);
                        return seg == FinalSegment;
                    }
                })
                .map(new MapFunction<String, Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple8<Long, Long, Integer, Integer,
                                Integer, Integer, Integer,
                                Integer> out = new Tuple8(Long.parseLong(fieldArray[0]),
                                Long.parseLong(fieldArray[1]), Integer.parseInt(fieldArray[2]),
                                Integer.parseInt(fieldArray[3]), Integer.parseInt(fieldArray[4]),
                                Integer.parseInt(fieldArray[5]), Integer.parseInt(fieldArray[6]),
                                Integer.parseInt(fieldArray[7]));
                        return out;
                    }
                });

        // Transforms element timestamp from ms to s

        KeyedStream<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = mapStream
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0 * 1000;
                            }
                        }
                ).keyBy(1);

        // Gets First Result

        SingleOutputStreamOperator<Tuple3< Long, Integer, Long>> first_Result = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(3600))).apply(new AverageSpeed());

        // Gets average speed

        SingleOutputStreamOperator<Tuple4<Long, Long, Integer, Long>> sumTumblingEventTimeWindows = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(3600))).apply(new AverageSpeed2());

        // Transforms element timestamp from ms to s

        KeyedStream<Tuple4<Long, Long, Integer, Long>, Tuple >  keyedStream2 = sumTumblingEventTimeWindows
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple4<Long, Long, Integer, Long>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple4<Long, Long, Integer, Long>  element) {
                                return element.f0 * 1000;
                            }
                        }
                ).keyBy(2);

        // Gets second  result

        SingleOutputStreamOperator<Tuple3<Long, Integer, Long>> second_Result = keyedStream2
                .window(TumblingEventTimeWindows.of(Time.seconds(3600))).apply(new GetOutput());

        first_Result.writeAsCsv(output1);
        second_Result.writeAsCsv(output2);

        // execute program
        env.execute("Exercise3");

    }

    //Input Tuple8
    //Output Tuple3

    public static class AverageSpeed implements WindowFunction<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>,
            Tuple3< Long, Integer, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple3< Long, Integer, Long>> out) throws Exception {

            Iterator<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Integer xWay = 0;
            Long numberOfVehicles = 0L;
            Long carID = 0L;
            Long avgSpeed = 0L;
            Integer speedCar = 0;

            if (first != null) {
                carID = first.f1;
                speedCar = first.f2;
                xWay = first.f3;
                numberOfVehicles = 1L;
                avgSpeed = speedCar / numberOfVehicles;

            }
            while (iterator.hasNext()) {
                Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                carID = next.f1;
                speedCar += next.f2;
                xWay = next.f3;
                numberOfVehicles += 1;
                avgSpeed = speedCar / numberOfVehicles;
            }

            out.collect(new Tuple3< Long, Integer, Long>( carID, xWay, avgSpeed));
        }
    }

    //Input Tuple8
    //Output Tuple4

    public static class AverageSpeed2 implements WindowFunction<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>,
            Tuple4<Long, Long, Integer, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple4<Long, Long, Integer, Long>> out) throws Exception {

            Iterator<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Integer xWay = 0;
            Long numberOfVehicles = 0L;
            Long carID = 0L;
            Long avgSpeed = 0L;
            Integer speedCar = 0;
            Long time = 0L;

            if (first != null) {
                time = first.f0;
                carID = first.f1;
                speedCar = first.f2;
                xWay = first.f3;
                numberOfVehicles = 1L;
                avgSpeed = speedCar / numberOfVehicles;
            }

            while (iterator.hasNext()) {
                Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                carID = next.f1;
                time = next.f0;
                speedCar += next.f2;
                xWay = next.f3;
                numberOfVehicles += 1;
                avgSpeed = speedCar / numberOfVehicles;
            }
            out.collect(new Tuple4<Long, Long, Integer, Long>(time, carID, xWay, avgSpeed));
        }
    }

    //Input Tuple4
    //Output Tuple4

    public static class GetOutput implements WindowFunction<Tuple4<Long, Long, Integer, Long>,
            Tuple3<Long, Integer, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, Long, Integer, Long>> input,
                          Collector<Tuple3<Long, Integer, Long>> out) throws Exception {

            Iterator<Tuple4<Long, Long, Integer, Long>> iterator = input.iterator();
            Tuple4<Long, Long, Integer, Long> first = iterator.next();

            Integer xWay = 0;
            Long carID = 0L;
            Long avgSpeed = 0L;

            if (first != null) {
                carID = first.f1;
                xWay = first.f2;
                avgSpeed = first.f3;
            }
            while (iterator.hasNext()) {

                Tuple4<Long, Long, Integer, Long> next = iterator.next();
                if (next.f3 > avgSpeed){
                    carID = next.f1;
                    xWay = next.f2;
                    avgSpeed = next.f3;
                }
            }
            out.collect(new Tuple3<Long, Integer, Long>(carID, xWay, avgSpeed));
        }
    }
}








