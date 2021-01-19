package es.upm.master;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
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

        // get all the user variables

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

        //filters out all observations that are not in exit lane
        //maps the input from string to correct type
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


        // transforms element timestamp from ms to s

        KeyedStream<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = mapStream
                    .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0*1000;
                            }
                        }
                ).keyBy(4);


        // gets number of cars that exit, per each hour

        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Long>> sumTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(3600))).apply(new TotalNumber());


        // emit result
            //mapStream.writeAsCsv(params.get("output"));
        sumTumblingEventTimeWindows.writeAsCsv(output);

        // execute program
        env.execute("Exercise1");
    }

    //input Tuple8
    //output Tuple4
    public static class TotalNumber implements WindowFunction<Tuple8<Long, Long, Integer, Integer, Integer, Integer, Integer, Integer>,
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
                iterator.next();
                numberOfVehicles += 1;
            }
            out.collect(new Tuple4<Long, Integer, Integer, Long>(time, xWay, exitLane, numberOfVehicles));
        }
    }
}
