/**
 * 
 */
package com.ringid.sparkStreaming.FlumeEventReceiver;

import java.io.Serializable;
import java.sql.Date;
import java.util.Map;

/**
 * @author kaidul
 *
 */
public class EventLog implements Serializable {

    // private static final long serialVersionUID = 6972814572157146958L;

    private Date date;
    private String logLevel;
    private String eventType;
    private String requestId;
    private String methodName;
    private Map<String, String> args;

    public EventLog() {
    }

    public EventLog(Date date, String logLevel, String eventType, String requestId, String methodName,
            Map<String, String> args) {
        this.date = date;
        this.logLevel = logLevel;
        this.eventType = eventType;
        this.requestId = requestId;
        this.methodName = methodName;
        this.args = args;
    }

    public EventLog(String logLevel, String methodName) {
        this.logLevel = logLevel;
        this.methodName = methodName;
    }

    @Override
    public String toString() {
        return Utilities.toString(this);
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Map<String, String> getArgs() {
        return args;
    }

    public void setArgs(Map<String, String> args) {
        this.args = args;
    }

}
