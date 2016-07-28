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
package com.baidu.cloud.bmr.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for AccessLogAnalyzer.
 *
 */
public class AccessLogAnalyzerMapper extends
        Mapper<Object, Text, Text, IntWritable> {

    private final String separator = "\\s{1}";
    private final String ipAddr = "(\\S+)";
    private final String remoteUser = "(.*?)";
    private final String timeLocal = "\\[(.*?)\\]";
    private final String request = "(.*?)";
    private final String status = "(\\d{3})";
    private final String bodyBytesSent = "(\\S+)";
    private final String httpReferer = "(.*?)";
    private final String httpCookie = "(.*?)";
    private final String httpUserAgent = "(.*?)";
    private final String requestTime = "(\\S+)";
    private final String host = "(\\S+)";
    private final String msec = "(\\S+)";

    private final String regex = ipAddr + separator + "-" + separator + timeLocal
            + separator + request + separator + status + separator
            + bodyBytesSent + separator + httpReferer + separator + httpCookie
            + separator + remoteUser + separator + httpUserAgent + separator
            + requestTime + separator + host + separator + msec;

    private final Text dateTime = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String logLine = value.toString();
        Pattern p = Pattern.compile(regex);
        Matcher matcher = p.matcher(logLine);
        if (matcher.matches()) {
            dateTime.set(matcher.group(2).split(":")[0]);
            context.write(dateTime, one);
        }
    }
}