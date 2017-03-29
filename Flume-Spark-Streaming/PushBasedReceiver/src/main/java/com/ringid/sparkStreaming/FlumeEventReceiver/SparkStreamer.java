package com.ringid.sparkStreaming.FlumeEventReceiver;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

/**
 * @author kaidul
 *
 */
@SuppressWarnings({ "serial", "deprecation" })
public class SparkStreamer {

    private static final EventLogParser eventLogParser = new EventLogParser();
    private static final Encoder<EventLog> eventLogEncoder = Encoders.bean(EventLog.class);
    private static final Logger logger = Logger.getLogger(SparkStreamer.class);
    
    private static Properties properties = new Properties();
    private static JavaSparkContext sparkContext;
    private static SQLContext sqlContext;

    private static final String CONFIG_FILE = "config.properties";
    private static final String APP_NAME_KEY = "spark.appname";
    private static final String CHECKPOINT_DIR = "spark.checkpoint.dir";
    private static final String CHECKPOINT_ENABLED = "spark.checkpoint.enabled";
    private static final String BATCH_INTERVAL_KEY = "spark.stream.batch_interval";
    private static final String COALESCE_PARTITION_KEY = "dataset.coalesce_partition";
    private static final String HDFS_PATH_KEY = "hdfs.path";
    private static final String WRITE_AHEAD_LOG_ENABLED = "spark.streaming.receiver.writeAheadLog.enable";
    private static final String TRUE = Boolean.toString(Boolean.TRUE);

    private static String appName;
    private static boolean isCheckpointEnabled;
    private static String checkpointDir;
    private static String isWalEnabled;
    private static int batchInterval;
    private static int partition;
    private static String hdfsPath;

    static {

        BasicConfigurator.configure();

        try {
            InputStream input;
            File file = new File(CONFIG_FILE);
            if (file.exists()) {
                input = new FileInputStream(file);
            } else {
                input = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_FILE);
            }
            properties.load(input);

            input.close();

        } catch (Exception ex) {
            logger.fatal("Exception reading config.properties file-->", ex);
        }

        appName = properties.getProperty(APP_NAME_KEY, "SparkFlumeStreaming");
        isCheckpointEnabled = Boolean.valueOf( properties.getProperty(CHECKPOINT_ENABLED, TRUE) );
        checkpointDir = properties.getProperty(CHECKPOINT_DIR, "hdfs://localhost:9000/user/flume-spark/checkpoint");
        isWalEnabled = properties.getProperty(WRITE_AHEAD_LOG_ENABLED, TRUE);
        batchInterval = Integer.valueOf(properties.getProperty(BATCH_INTERVAL_KEY, "10"));
        partition = Integer.valueOf(properties.getProperty(COALESCE_PARTITION_KEY, "1"));
        hdfsPath = properties.getProperty(HDFS_PATH_KEY, "hdfs://localhost:9000/user/flume-spark/eventlog");

        logger.info("Application Name: " + appName);
        logger.info("HDFS Directory: " + hdfsPath);
    }
    
    
    private static JavaStreamingContext createContext() {
        
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        if(isCheckpointEnabled) {
            sparkConf.set(WRITE_AHEAD_LOG_ENABLED, TRUE);
        } else {
            sparkConf.set(WRITE_AHEAD_LOG_ENABLED, isWalEnabled);
        }
        sparkContext = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(sparkContext);
        
        return new JavaStreamingContext(sparkContext, Durations.seconds(batchInterval));
    }
    
    
    private static JavaDStream<String> createDStream(JavaStreamingContext javaStreamingContext, String hostName, int port) {
        
        JavaReceiverInputDStream<SparkFlumeEvent> flumeEventStream = FlumeUtils.createStream(javaStreamingContext, hostName, port);
//        flumeEventStream.persist(StorageLevel.MEMORY_AND_DISK_SER());
        
        JavaDStream<String> dStream = flumeEventStream.map(new Function<SparkFlumeEvent, String>() {

            @Override
            public String call(SparkFlumeEvent sparkFlumeEvent) throws Exception {

                byte[] bodyArray = sparkFlumeEvent.event().getBody().array();
                String logTxt = new String(bodyArray, "UTF-8");
                logger.info(logTxt);

                return logTxt;
            }
        });
        // dStream.print();
        
        return dStream;
    }
    
    
    private static void writeToHdfs(JavaRDD<EventLog> logRdd) {
        
        Dataset<EventLog> logDataset = sqlContext.createDataset(logRdd.collect(), eventLogEncoder);

        // logDataset.show();
        logDataset.coalesce(partition)
                  .write()
                  .mode(SaveMode.Append)
                  .parquet(hdfsPath);
    }
    
    
    private static void processDStream(JavaDStream<String> dStream) {
        
        dStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {

            @Override
            public void call(JavaRDD<String> rdd) throws Exception {

                if (rdd.isEmpty()) {
                    return;
                }

                JavaRDD<EventLog> logRdd = rdd.map(new Function<String, EventLog>() {

                    @Override
                    public EventLog call(String logText) throws Exception {

                        EventLog eventLog = eventLogParser.parseLog(logText);
                        logger.info(eventLog);

                        return eventLog;
                    }

                });

                writeToHdfs(logRdd);

            }
        });
    }
    

    public static void main(String[] args) {

        if (args.length != 2) {
            logger.error("Usage: " + appName + " <host> <port>");
            System.exit(1);
        }

        final String hostName = args[0];
        final int port = Integer.parseInt(args[1]);
        
        JavaStreamingContext javaStreamingContext = null;
        
        if(isCheckpointEnabled) {
            
            javaStreamingContext = JavaStreamingContext.getOrCreate(checkpointDir, new Function0<JavaStreamingContext>() {

                @Override
                public JavaStreamingContext call() throws Exception {

                    JavaStreamingContext jStreamingCtx = createContext();

                    jStreamingCtx.checkpoint(checkpointDir);

                    JavaDStream<String> dStream = createDStream(jStreamingCtx, hostName, port);

                    processDStream(dStream);

                    return jStreamingCtx;
                }
            });
        }
        else {
            javaStreamingContext = createContext();

            JavaDStream<String> dStream = createDStream(javaStreamingContext, hostName, port);

            processDStream(dStream);
        }
        

        
        javaStreamingContext.start();

        try {
            javaStreamingContext.awaitTermination();
            
        } catch (InterruptedException ex) {
            logger.warn(ex);
        } finally {
            // System.exit(0);
        }
    }
}