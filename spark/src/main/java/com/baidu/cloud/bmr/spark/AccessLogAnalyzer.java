/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 BeiJing Baidu Netcom Science Technology Co., Ltd
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.baidu.cloud.bmr.spark;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Tuple2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;

/**
 * Analyze log with Spark.
 */
public class AccessLogAnalyzer {

    private static final SimpleDateFormat logDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

    private static String fetchDate(String gmsTime) {
        String date;
        try {
            date = simpleDateFormat.format(logDateFormat.parse(gmsTime));
        } catch (ParseException e) {
            date = null;
        }
        return date;
    }

    private static final Pattern LOGPATTERN = Pattern.compile(
            "(\\S+)\\s+-\\s+\\[(.*?)\\]\\s+\"(.*?)\"\\s+(\\d{3})\\s+(\\S+)\\s+"
            + "\"(.*?)\"\\s+\"(.*?)\"\\s+(.*?)\\s+\"(.*?)\"\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)");

    private static Tuple2<String, String> extractKey(String line) {
        Matcher m = LOGPATTERN.matcher(line);
        if (m.find()) {
            String ipAddr = m.group(1);
            String date = fetchDate(m.group(2));
            return new Tuple2<>(date, ipAddr);
        }
        return new Tuple2<>(null, null);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("usage: spark-submit com.baidu.cloud.bmr.spark.AccessLogAnalyzer <input> <pv> <uv>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("AccessLogAnalyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parses the log to log records and caches the result.
        JavaPairRDD<String, String> distFile = sc.textFile(args[0]).mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) {
                        return extractKey(s);
                    }
                });
        distFile.cache();

        // Changes the log info to (date, 1) format, and caculates the page view.
        JavaPairRDD<String, Integer> pv = distFile.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, String> tuple) {
                        return new Tuple2<>(tuple._1(), 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });
        // Coalesce to 1 partition and save as file. Notice that this is for demo purpose only.
        pv.coalesce(1, true).saveAsTextFile(args[1]);

        // Changes the log info to (date, remoteAddr) and caculates the unique visitors.
        JavaPairRDD<String, Integer> uv = distFile.groupByKey().mapToPair(
                new PairFunction<Tuple2<String, Iterable<String>>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Iterable<String>> tuple) {
                        int size = new HashSet((Collection<?>) tuple._2()).size();
                        return new Tuple2<>(tuple._1(), size);
                    }
                });
        // Coalesce to 1 partition and save as file. Notice that this is for demo purpose only.
        uv.coalesce(1, true).saveAsTextFile(args[2]);
    }
}
