/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparkstreamingtemplete;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

/**
 *
 * @author rakib
 */
public class SparkStreamer {

    private static JavaSparkContext sparkContext;
    private static SQLContext sqlContext;

    private static JavaStreamingContext createContext() {
        SparkConf conf = new SparkConf().setAppName("mySparkStreamingApp");
        sparkContext = new JavaSparkContext(conf);
        sqlContext = new SQLContext(sparkContext);
        return new JavaStreamingContext(sparkContext, Durations.seconds(1));
    }

    private static JavaDStream<String> createDStream(JavaStreamingContext javaStreamingContext, String hostname, int port) {
        JavaReceiverInputDStream<SparkFlumeEvent> flumeEventStream = FlumeUtils.createStream(javaStreamingContext, hostname, port);

        JavaDStream<String> dStream = flumeEventStream.map(new Function<SparkFlumeEvent, String>() {
            @Override
            public String call(SparkFlumeEvent sparkFlumeEvent) throws Exception {
                //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                byte[] bodyArray = sparkFlumeEvent.event().getBody().array();
                String logTxt = new String(bodyArray, "UTF-8");
                return logTxt;
            }
        });
        return dStream;
    }

    private static void processDStream(JavaDStream<String> dStream) {
        dStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                if (rdd.isEmpty()) {
                    return;
                }
                //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        });

    }

    private static void writeDStream(String logRdd) {

    }

    public static void main(String[] args) {

        final String hostname = args[0];
        final int port = Integer.parseInt(args[1]);

        if (args.length != 2) {
            System.exit(1);
        }

        //initialize streaiming context
        JavaStreamingContext javaStreamingContext = null;

        javaStreamingContext = createContext();

        //Define input source by creating input DStream
        JavaDStream<String> dStream = createDStream(javaStreamingContext, hostname, port);

        //Process DStream
        processDStream(dStream);
        
        javaStreamingContext.start();
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException ex) {

        } finally {
            //System.exit(0);
        }
    }
}
