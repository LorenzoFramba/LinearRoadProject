package es.upm.master;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple4;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Iterator;

//Develop a Java program using Flink to calculate the number of cars that use each exit lane every hour.
public class exercise1 {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();;

        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        String input = "";
        String output = "";
        try {
            input = params.get("input");
            output = params.get("output");
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Input file and output folder path must be provided as argument to this program. Aborting...");
            return;
        }


        DataStream<String> text = env.readTextFile(input);

        SingleOutputStreamOperator< Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = text
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        String[] fieldArray = value.split(",");
                        int exit = Integer.parseInt(fieldArray[4]);
                        return exit == 4;
                    }})
                .map(new MapFunction<String, Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> >() {
                    public Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>  map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple8<Long, Long, Integer, Integer,
                                Integer, Integer, Integer,
                                Integer>  out = new Tuple8(Long.parseLong(fieldArray[0]),
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
                                return element.f0*1000;
                            }
                        }
                ).keyBy(4);


        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Long>> sumTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(3600))).apply(new SimpleSum());


        // emit result
            //mapStream.writeAsCsv(params.get("output"));
        sumTumblingEventTimeWindows.writeAsCsv(output);

        // execute program
        env.execute("Exercise1");

    }

    private  Vehicle cellSplitter(String line) {
        String[] cells = line.split(",");
        return new Vehicle(Long.parseLong(cells[0]), Long.parseLong(cells[1]),
                Integer.parseInt(cells[2]), Integer.parseInt(cells[3]), Integer.parseInt(cells[4]), Integer.parseInt(cells[5]),
                Integer.parseInt(cells[6]), Integer.parseInt(cells[7]));
    }

    public static class SimpleSum implements WindowFunction<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>,
                                                Tuple4<Long, Integer, Integer, Long>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                          Collector<Tuple4<Long, Integer, Integer, Long>> out) throws Exception {

            Iterator<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();

            Long time = 0L;
            Integer xWay =0;
            Integer exitLane =0;
            Long numberOfVehicles = 0L;

            if(first!=null){
                time = first.f0;
                xWay = first.f3;
                exitLane = first.f4;
                numberOfVehicles = 1L;

            }
            while(iterator.hasNext()){
                Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();

                numberOfVehicles += 1;
            }
            out.collect(new Tuple4<Long, Integer, Integer, Long>(time, xWay, exitLane,numberOfVehicles));
        }
    }

    public class Vehicle extends Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> {

        public Vehicle(final Long Time, final Long VID, final Integer Spd,
                       final Integer XWay, final Integer Lane, final Integer Dir,
                       final Integer Seg, final Integer Pos) {
            this.f0 = Time; //timestamp
            this.f1 = VID; //vehicleId
            this.f2 = Spd;  //speed
            this.f3 = XWay; //highwayId
            this.f4 = Lane; //Lane
            this.f5 = Dir; //direction
            this.f6 = Seg; //segment
            this.f7 = Pos; //position
        }
        public Long getTimestamp() {
            return this.f0;
        }

        public Long getVehicleId() {
            return this.f1;
        }

        public Integer getSpeed() {
            return this.f2;
        }

        public Integer getHighwayId() {
            return this.f3;
        }

        public Integer getLane() {
            return this.f4;
        }

        public Integer getDirection() {
            return this.f5;
        }
        public Integer getSegment() {
            return this.f6;
        }

        public Integer getPosition() {
            return this.f7;
        }




    }






}
