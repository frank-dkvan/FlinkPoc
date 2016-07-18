package org.apache.flink.quickstart;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by Gennady.Gilin on 6/19/2016.
 */
public class RideSpeed {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.getRequired("input");

        final int maxEventDelay = 60; // events are out of order by max 60 seconds
        final float servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

        DataStream<Tuple2<Long, Float>> rideSpeeds = rides
                // filter out rides that do not start or stop in NYC
                .filter(new RideCleansing.NYCFilter())
                // group records by rideId
           //     .keyBy("rideId")
                // compute the average speed of a ride
                .flatMap(new SpeedComputer());

        // print the result to stdout
        rideSpeeds.print();

        // run the transformation pipeline
        env.execute("Average Ride Speed");
    }

    /**
     * Computes the average speed of a taxi ride
     *
     */
    public static class SpeedComputer extends RichFlatMapFunction<TaxiRide, Tuple2<Long, Float>> {

        private ValueState<TaxiRide> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = this.getRuntimeContext().getState(new ValueStateDescriptor<>("ride", TaxiRide.class, null));
        }

        @Override
        public void flatMap(TaxiRide taxiRide, Collector<Tuple2<Long, Float>> out)
                throws Exception {

            if(state.value() == null) {
                // we received the first element. Put it into the state
                state.update(taxiRide);
            }
            else {
                // we received the second element. Compute the speed.
                TaxiRide startEvent = taxiRide.isStart ? taxiRide : state.value();
                TaxiRide endEvent = taxiRide.isStart ? state.value() : taxiRide;

                long timeDiff = endEvent.time.getMillis() - startEvent.time.getMillis();
                float avgSpeed;

                if(timeDiff != 0) {
                    // speed = distance / time
                    avgSpeed = (endEvent.travelDistance / timeDiff) * (1000 * 60 * 60);
                }
                else {
                    avgSpeed = -1f;
                }
                // emit average speed
                out.collect(new Tuple2<>(startEvent.rideId, avgSpeed));

                // clear state to free memory
                state.update(null);
            }
        }
    }

}
