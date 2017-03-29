package com.ringid.sparkStreaming.FlumeEventReceiver;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author kaidul
 *
 */
public class EventLogParser {

    // 20170201210506187 INFO - R 1b5eb5b0-e8c2-11e6-aeba-816202914bb1 getMyFriends - {"userId":28,"userUpdateTime":1485862991848,"contactUpdateTime":1485932434083,"countryCode":"+880"}

    private static Pattern logPattern = Pattern.compile(
            "^(\\d{17})\\s+(\\w+)\\s*-?\\s*(R|\\d+)\\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\\s+(\\w+)\\s*-?\\s*\\{{1}(.*?)\\}{1})?$");

    private static Pattern keyValuePattern = Pattern.compile("\"?(\\w+)\"?\\s*:\\s*\"?(\\w+)\"?");

    private static String datePattern = "yyyyMMddHHmmssSSS"; // e.g. 20170201210453812

    public EventLog parseLog(String logtxt) {
        Matcher logMatcher = logPattern.matcher(logtxt);
        if (!logMatcher.matches()) {
            return null;
        }
        String timestamp = logMatcher.group(1);
        String logLevel = logMatcher.group(2);
        String eventType = logMatcher.group(3);
        String requestId = logMatcher.group(4);
        String methodName = logMatcher.group(6);
        String funcArgs = logMatcher.group(7);

        SimpleDateFormat dateFormat = new SimpleDateFormat(datePattern);
        Date date = null;
        try {
            date = dateFormat.parse(timestamp);
        } catch (ParseException ex) {
            System.out.println(ex);
        }
        java.sql.Date sqlDate = new java.sql.Date(date.getTime());

        Map<String, String> args = null;

        if (funcArgs != null) {
            Matcher keyValueMatcher = keyValuePattern.matcher(funcArgs);
            args = new HashMap<>();

            while (keyValueMatcher.find()) {
                String key = keyValueMatcher.group(1);
                String value = keyValueMatcher.group(2);
                args.put(key, value);
            }
        }

        return new EventLog(sqlDate, logLevel, eventType, requestId, methodName, args);
    }

}
