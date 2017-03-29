package com.ringid.test.SparkStreamTest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ringid.sparkStreaming.FlumeEventReceiver.EventLog;
import com.ringid.sparkStreaming.FlumeEventReceiver.EventLogParser;

public class SparkStreamTest {

    private static String log = "20170201210506187 INFO - R 1b5eb5b0-e8c2-11e6-aeba-816202914bb1 getMyFriends - {\"userId\":28,\"userUpdateTime\":1485862991848,\"contactUpdateTime\":1485932434083,\"countryCode\":\"+880\"}";
    private static String log2 = "20170201210453789 INFO -18 13f68000-e8c2-11e6-aeba-816202914bb1";
    private static final EventLogParser eventLogParser = new EventLogParser();

    @Before
    public void setup() {

    }

    @Test
    public void testEventLogParser() {
        EventLog eventLog = eventLogParser.parseLog(log2);
        Assert.assertTrue(eventLog != null);
        System.out.println(eventLog);

        eventLog = eventLogParser.parseLog(log);
        System.out.println(eventLog);
    }

}
