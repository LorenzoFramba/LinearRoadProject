package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
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

        // set up the execution environment

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();







        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String input = "";
        String output = "";
        int speed = 0;
        long time = 0L;
        int startSegment = 0;
        int endSegment = 0;

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
        DataStream<String> text = env.readTextFile(input);

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

        KeyedStream<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = mapStream
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0 * 1000;
                            }
                        }
                ).keyBy(3);


        SingleOutputStreamOperator<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> reduce = keyedStream.reduce(
                new ReduceFunction<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>>() {


                    public Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> value1,
                                                                                                           Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> value2) throws Exception {

                        Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple3<Long, String, Double>(value2.f0, value1.f1,value1.f2+value2.f2);
                        return out;
                    }
                }
        );




        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Long>> sumTumblingEventTimeWindows = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(time))).apply(new SimpleSum());

        // emit result

        mapStream.writeAsCsv(output);


        // execute program
        env.execute("Exercise2");

    }


    private static class averageSpeedFunction implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> collector) throws Exception {
            boolean segA = false, segB = false;
            Tuple stream;
            List<Integer> timestamp = new ArrayList<Integer>();
            List<Integer> Distance = new ArrayList<Integer>();
            List<Integer> speed = new ArrayList<Integer>();
            int Time2 = 0, Time1 = Integer.MAX_VALUE;
            int Distance1= Integer.MAX_VALUE, Distance2= 0;

            OptionalDouble avgSpeed = OptionalDouble.of(0);
            Double avgSpeedms =0.0;
            Double avgSpeedFinal =0.0;
            for (Tuple8 element : iterable) {
                if ((int) element.f6 == 52) {
                    segA = true;
                } else if ((int) element.f6 == 56) {
                    segB = true;
                }

                speed.add((int) element.f2);
                timestamp.add((int) element.f0);
                Distance.add((int) element.f7);

                if (segA && segB) {
                    Time2 = Collections.max(timestamp);
                    Time1 = Collections.min(timestamp);

                    Distance2 = Collections.max(Distance);
                    Distance1 = Collections.min(Distance);

                    //Double variable = Calculate the speed which velocity = distance2 - distance 1 / time2 - time1
                    //Distnace*2.237

                    avgSpeedms = (Distance2-Distance1)/(Time2-Time1)+0.0;
                    avgSpeedFinal = avgSpeedms*2.23694;
                    //counter +1
                }
                //Divide sum of velocity/count
            }
            if(avgSpeedFinal > 60.0)
                collector.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>(Time1, Time2, Integer.parseInt(tuple.getField(0).toString()), Integer.parseInt(tuple.getField(1).toString()), Integer.parseInt(tuple.getField(2).toString()), avgSpeedFinal));
        }
    }


    public static class SimpleSum implements WindowFunction<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>,
            Tuple4<Long, Integer, Integer, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple4<Long, Integer, Integer, Long>> out) throws Exception {

            Iterator<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();

            Long time = 0L;
            Integer xWay = 0;
            Integer exitLane = 0;
            Long numberOfVehicles = 0L;

            if (first != null) {
                time = first.f0;
                xWay = first.f3;
                exitLane = first.f4;
                numberOfVehicles = 1L;

            }
            while (iterator.hasNext()) {
                Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();

                numberOfVehicles += 1;
            }
            out.collect(new Tuple4<Long, Integer, Integer, Long>(time, xWay, exitLane, numberOfVehicles));
        }
    }
}


