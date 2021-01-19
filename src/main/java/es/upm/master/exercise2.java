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
import java.util.*;

import java.util.Iterator;

public class exercise2 {


    public static void main(String[] args) throws Exception {


        final ParameterTool params = ParameterTool.fromArgs(args);

        // Sets up the execution environment

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String input = "";
        String output = "";


        long time = 0L;
        int startSegment = 0;
        int endSegment = 0;
        int speed = 0;

        // Gets all the user variables

        try {
            input = params.get("input");
            output = params.get("output");
            speed = Integer.parseInt(params.get("speed"));
            time = Long.parseLong(params.get("time"));
            startSegment =  Integer.parseInt(params.get("startSegment"));
            endSegment =  Integer.parseInt(params.get("endSegment"));
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Input file and output folder path must be provided as argument to this program. Aborting...");
            return;
        }

        final int startSegment2 = startSegment;
        final int endSegment2 = endSegment;
        final int innerSpeed = speed;
        DataStream<String> text = env.readTextFile(input);

        // Filters out all observations that are go to different direction and are in different segments than the chosen ones
        // Maps the input from string to correct type

        SingleOutputStreamOperator<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = text
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        String[] fieldArray = value.split(",");
                        int direction = Integer.parseInt(fieldArray[5]);
                        return direction == 0;
                    }
                })
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        String[] fieldArray = value.split(",");
                        int seg = Integer.parseInt(fieldArray[6]);
                        return seg >= startSegment2 && seg <= endSegment2;
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

        // Calculates the average speed of each car
        // Filters out all observations with AvgSpeed lower or equal than requested.

        SingleOutputStreamOperator<Tuple4<Long, Integer,  Long, Long>> sumTumblingEventTimeWindows = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(time))).apply(new AverageSpeed())
                .filter(new FilterFunction<Tuple4<Long, Integer,  Long, Long>>() {
                    public boolean filter(Tuple4<Long, Integer,  Long, Long> in) throws Exception {
                        return in.f2 > innerSpeed;
                    }
                });

        // Transforms element timestamp from ms to s

        KeyedStream<Tuple4<Long, Integer,  Long, Long>, Tuple> keyedStream2 = sumTumblingEventTimeWindows
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple4<Long, Integer,  Long, Long>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple4<Long, Integer,  Long, Long> element) {
                                return element.f0 * 1000;
                            }
                        }
                ).keyBy(1);

        // Gets resulting carsID, divided in lists, per each TimeFrame

        SingleOutputStreamOperator<Tuple4<Long, Integer,  Long, String>> sumTumblingEventTimeWindows2 = keyedStream2
                .window(TumblingEventTimeWindows.of(Time.seconds(time))).apply(new GetOutput());

        // Emits result
        sumTumblingEventTimeWindows2.writeAsCsv(output);


        // Executes program
        env.execute("Exercise2");

    }


    //Input Tuple8
    //Output Tuple4
    public static class AverageSpeed implements WindowFunction<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>,
            Tuple4<Long, Integer,  Long, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple4<Long, Integer,  Long, Long>> out) throws Exception {
            Iterator<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();

            Long time = 0L;
            Integer xWay = 0;

            Long numberOfVehicles = 0L;
            Long carID = 0L;
            Long avgSpeed = 0L;
            Integer speedCar = 0;

            if (first != null) {
                carID = first.f1;
                speedCar = first.f2;
                time = first.f0;
                numberOfVehicles = 1L;
                avgSpeed = speedCar / numberOfVehicles;
                xWay = first.f3;
            }

            while (iterator.hasNext()) {
                Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                xWay = first.f3;
                carID = next.f1;
                speedCar += next.f2;
                numberOfVehicles += 1;
                avgSpeed = speedCar / numberOfVehicles;
            }

            out.collect(new Tuple4<Long, Integer,  Long, Long>(time, xWay, avgSpeed, carID));
        }
    }


    //Input Tuple4
    //Output Tuple4
    public static class GetOutput implements WindowFunction<Tuple4<Long, Integer,  Long, Long>,
            Tuple4<Long, Integer,  Long, String>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, Integer,  Long, Long>> input,
                          Collector<Tuple4<Long, Integer,  Long, String>> out) throws Exception {

            Iterator<Tuple4<Long, Integer,  Long, Long>> iterator = input.iterator();
            Tuple4<Long, Integer,  Long, Long> first = iterator.next();
            List<Long> CarsId = new ArrayList<Long>();

            Long numberOfVehicles = 0L;
            Long time = 0L;
            Integer xWay = 0;
            Long carID = 0L;
            String carList = "[ ";

            if (first != null) {

                time = first.f0;
                xWay = first.f1;
                carID = first.f3;
                numberOfVehicles = 1L;
                CarsId.add(carID);
            }

            while (iterator.hasNext()) {

                Tuple4<Long, Integer,  Long, Long> next = iterator.next();

                if (next.f0 < time)
                    time = next.f0;
                xWay = next.f1;
                carID = next.f3;
                numberOfVehicles += 1;
                CarsId.add(carID);
            }


            for(int i = 0; i < CarsId.size(); i++) {
                carList += CarsId.get(i);
                if (i != (CarsId.size() - 1)){
                    carList +=" - ";
                }
            }
            carList += " ]";

            out.collect(new Tuple4<Long, Integer,  Long, String>(time, xWay, numberOfVehicles, carList));
        }
    }
}


