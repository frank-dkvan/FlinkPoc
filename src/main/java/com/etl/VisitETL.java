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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class VisitETL {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.getRequired("input");
		final String output = params.getRequired("output");

		final int maxEventDelay = 0; // events are out of order by max 60 seconds
		final float servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime );

		// start the data generator
		DataStream<PageViewRecord> allEvents = env.addSource(
				new PageViewSource( input, maxEventDelay, servingSpeedFactor));

		DataStream<PageViewRecord> pageViewEvents = allEvents
				// filter out rides that do not start or stop in NYC
				.filter( new PageViewFilter() );

		System.out.println( "Found page views - " + PageViewFilter.counter );

		// print the filtered stream
		pageViewEvents.print();
		pageViewEvents.writeAsText( "pageviews", FileSystem.WriteMode.OVERWRITE );

		CsvOutputFormat<PageViewRecord> csvOutputFormat = new CsvOutputFormat<PageViewRecord>( new Path("events"));
		csvOutputFormat.setWriteMode( FileSystem.WriteMode.OVERWRITE );
	//	pageViewEvents.writeUsingOutputFormat( csvOutputFormat );

//		RollingSink sink = new RollingSink<String>("c:\\ClickTale\\POC\\Flink\\output\\");
//		sink.setBucketer( new DateTimeBucketer("yyyy-MM-dd--HHmm"));
//		sink.setBatchSize( 1024 ); // this is 400 MB,
//		filteredRides.addSink( sink );

		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");

	}


	public static class PageViewFilter implements FilterFunction<PageViewRecord> {

		private static int counter = 0;
		@Override
		public boolean filter( PageViewRecord pageViewRecord ) throws Exception {

			counter++;
			return pageViewRecord.getEntityType() == 0 ? true : false;
		}
	}



}
