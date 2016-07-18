package com.etl;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.clicktale.data.PageViewRecord;
import com.data.source.PageViewSource;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Properties;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class PageViewTransform {
	//
	//	Program
	//
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.getRequired("input");
		final String output = params.getRequired("output");

		final int maxEventDelay = 60; // events are out of order by max 60 seconds
		final float servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();//StreamExecutionEnvironment.getExecutionEnvironment(); //
//		env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime );

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.223:9092");
// only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "192.168.180.223:2181");
        properties.setProperty("group.id", "flink-group");
      //  properties.setProperty("partition.assignment.strategy", "roundrobin ");

		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer08<>("flink-topic5", new SimpleStringSchema(), properties));

      //  stream.rebalance().print();



		// start the data generator
//		DataStream<PageViewRecord> rides = env.addSource(
//				new PageViewSource(input, maxEventDelay, servingSpeedFactor));

//		DataStream<PageViewRecord> filteredRides = rides
//				// filter out rides that do not start or stop in NYC
//				.filter( new PageViewFilter() );
//		DataStream<Tuple1<Integer>> popularSpots = rides
//				// remove all rides which are not within NYC
//				.map( new CellMapper() );


		RollingSink sink = new RollingSink<String>("c:\\ClickTale\\POC\\Flink\\output\\");
		sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"));
		sink.setWriter(new StringWriter<String>());
//		sink.setBatchSize( 1024 ); // this is 400 MB,

	//	popularSpots.addSink( sink );

        stream.rebalance().addSink( sink );

        // print the filtered stream
		//filteredRides.print();
	//	filteredRides.addSink( sink );

	//	filteredRides.writeAsText( output);

	//	popularSpots.writeAsCsv( output );
		// run the cleansing pipeline
		env.execute("Test");


	}

	/**
	 * Map taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrigggval location.
	 */
	public static class CellMapper implements MapFunction<PageViewRecord, Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map( PageViewRecord pageViewRecord ) throws Exception {

			Tuple1<Integer> result = new Tuple1<>();
			result.setField( pageViewRecord.getEntityType(), 0 );

			return result;
		}
	}

	/**
	 * Map taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class GridCellMatcher implements MapFunction<PageViewRecord, Tuple11<Integer,    // entity type
																					   Integer,	  // project id
																					   Integer,    // visitor id
																					   Integer, // visit id
																					   Integer, // pageview Id
																					   String,    // create date
																					   String,    // page view date
																					   String,    // location schema
																					   String,	  // location body
																					   String,    // location parameteres
																					   String>>{ // event ids

		@Override
		public Tuple11<Integer, Integer, Integer, Integer, Integer, String, String, String, String, String, String> map( PageViewRecord pageViewRecord ) throws Exception {

				Tuple11<Integer, Integer, Integer, Integer, Integer, String, String, String, String, String, String> result = new Tuple11<>();

				if( pageViewRecord.getTimestamp() != null ){

					Date date = new Date( pageViewRecord.getTimestamp() );

					DateTimeFormatter format = DateTimeFormatter.ofPattern("MMM dd yyyy hh:mm a" );
					LocalDateTime localDateTime = LocalDateTime.ofInstant( date.toInstant(), ZoneId.systemDefault());
					String pageViewData = localDateTime.format( format );

					result.setField( pageViewData, 6 );

					String creationDate = LocalDateTime.now().format( format );
					result.setField( creationDate, 5 );
				}
				result.setField( pageViewRecord.getEntityType(), 0 );
				result.setField( pageViewRecord.getProjectId(), 1 );
				result.setField( pageViewRecord.getVisitorId(), 2 );
				result.setField( pageViewRecord.getVisitId(), 3 );
				result.setField( pageViewRecord.getPageviewId(), 4 );
				result.setField( "http", 7 );
				result.setField( pageViewRecord.getLocation(), 8 );
				result.setField( pageViewRecord.getLocation(), 9 );
				result.setField( pageViewRecord.getEventIds(), 10 );

				return result;
			}
	}


	public static class PageViewFilter implements FilterFunction<PageViewRecord> {

		private static int counter = 0;
		@Override
		public boolean filter( PageViewRecord pageViewRecord ) throws Exception {

			System.out.println( ++counter );
			return pageViewRecord.getEntityType() == 1 ? true : false;
		}
	}


}
