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
import com.clicktale.utils.Utils;
import com.data.source.PageViewRecordSchema;
import com.data.source.PageViewSource;
import com.data.writer.PageViewEventsWriter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
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
public class EtlPoc {
	//
	//	Program
	//
	public static void main(String[] args) throws Exception {

//		ParameterTool params = ParameterTool.fromArgs(args);
//		final String input = params.getRequired("input");
//		final String output = params.getRequired("output");

		final int maxEventDelay = 60; // events are out of order by max 60 seconds
		final float servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        String allEventsFolder = "";
        String visitsFolder = "";

        if( Utils.isWindows() ){

            allEventsFolder = "c:\\ClickTale\\POC\\Flink\\allevents\\";
            visitsFolder = "c:\\ClickTale\\POC\\Flink\\visits\\";
        }else{

            allEventsFolder = "/opt/data/events";
            visitsFolder = "/opt/data/visits";
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.180.223:9092");
// only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "192.168.180.223:2181");
        properties.setProperty("group.id", "flink-group");
        //  properties.setProperty("partition.assignment.strategy", "roundrobin ");

        //DataStream<String> stream = env.addSource(new FlinkKafkaConsumer08<>("flink-topic1", new SimpleStringSchema(), properties));
        DataStream<PageViewRecord> allEvents = env.addSource(new FlinkKafkaConsumer08<PageViewRecord>("flink-topic5", new PageViewRecordSchema(), properties));

        RollingSink allEventsSink = new RollingSink<String>( allEventsFolder );

        allEventsSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"));
        allEventsSink.setWriter(new StringWriter<PageViewRecord>());

        allEvents.addSink( allEventsSink );

        // end of all events processing -------------------------------------------------------------------
        // only page view events

        DataStream<PageViewRecord> pageViewEvents = allEvents
                // filter out rides that do not start or stop in NYC
                .filter( new PageViewFilter() );
       // pageViewEvents.print();

        RollingSink pageEventsSink = new RollingSink<PageViewRecord>( visitsFolder );

        pageEventsSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HH-mm"));
        pageEventsSink.setWriter(new PageViewEventsWriter<PageViewRecord>());


        pageViewEvents.addSink( pageEventsSink );

        // end of page events processing ---------------------------------------------------------------------

		env.execute("ETL POC");
	}


	public static class PageViewFilter implements FilterFunction<PageViewRecord> {

		private static int counter = 0;
        private static int total = 0;
        private static long startTimer = Calendar.getInstance().getTimeInMillis();

        @Override
		public boolean filter( PageViewRecord pageViewRecord ) throws Exception {

            boolean ret = false;

            if( pageViewRecord.getEntityType() == 1 ){

             //   System.out.println("visits ->" + ++counter );
                ret = true;
            }

            if( ( ++total % 10000 ) == 0 ){

                Long seconds = ( Calendar.getInstance().getTimeInMillis() - startTimer ) / 1000;
                Float averageProcessingTime = (float)total / (float)seconds;
                System.out.println("Processed total [" + total + "], average processing time - " + averageProcessingTime + "/sec");
            }

			return ret;
		}
	}


}
