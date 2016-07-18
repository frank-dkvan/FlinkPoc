package org.apache.flink.quickstart;

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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

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
public class MailProcessor {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//		DataSet<Tuple6<String, String, String, String, String, String>> mails =
//				env.readCsvFile("c:\\ClickTale\\POC\\Flink\\flinkMails.gz")
//						.lineDelimiter("##//##")
//						.fieldDelimiter("#|#")
//						.types(String.class, String.class, String.class,
//								String.class, String.class, String.class);

// read sender and body fields
		DataSet<Tuple2<String, String>> timeSender =
				env.readCsvFile("c:\\ClickTale\\POC\\Flink\\flinkMails.gz")
						.lineDelimiter("##//##")
						.fieldDelimiter("#|#")
						.includeFields("011")
						.types(String.class, String.class);

        timeSender.map( new MonthEmailExtractor() )
				// group by month and email address and count number of records per group
				.groupBy(0, 1).reduceGroup(new MailCounter())
				// print the result
				.print();
		//env.execute("Word count example");

		// execute and print result
	//	timeSender.print();

		System.out.println( "Found total records - " + timeSender.count() );
		//counts.writeAsCsv( args[1], "\n", "");
	}
	/**
	 * Extracts the month from the time field and the email address from the sender field.
	 */
	public static class MonthEmailExtractor implements MapFunction<Tuple2<String, String>, Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> map(Tuple2<String, String> mail) throws Exception {

			// extract year and month from time string
			String month = mail.f0.substring(0, 7);
			// extract email address from the sender
			String email = mail.f1.substring(mail.f1.lastIndexOf("<") + 1, mail.f1.length() - 1);

			return new Tuple2<>(month, email);
		}
	}
	/**
	 * Counts the number of mails per month and email address.
	 */
	public static class MailCounter implements GroupReduceFunction<Tuple2<String ,String>, Tuple3<String ,String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<String, String>> mails, Collector<Tuple3<String, String, Integer>> out) throws Exception {

			String month = null;
			String email = null;
			int cnt = 0;

			// count number of tuples
			for(Tuple2<String, String> m : mails) {
				// remember month and email address
				month = m.f0;
				email = m.f1;
				// increase count
				cnt++;
			}

			// emit month, email address, and count
			out.collect(new Tuple3<>(month, email, cnt));
		}
	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
