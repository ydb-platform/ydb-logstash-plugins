package org.logstashplugins.util;

import co.elastic.logstash.api.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class CustomEvent implements Event {
    private static final Logger logger = LoggerFactory.getLogger(CustomEvent.class);
    private final Map<String, Object> map = new HashMap<>();

    @Override
    public Map<String, Object> getData() {
        return map;
    }

    @Override
    public Map<String, Object> getMetadata() {
        return null;
    }

    @Override
    public void cancel() {

    }

    @Override
    public void uncancel() {

    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public Instant getEventTimestamp() {
        return null;
    }

    @Override
    public void setEventTimestamp(Instant instant) {

    }

    @Override
    public Object getField(String s) {
        return null;
    }

    @Override
    public Object getUnconvertedField(String s) {
        return null;
    }

    @Override
    public void setField(String s, Object o) {
        logger.info("Adding in map object with key: " + s + " and data: " + o);
        map.put(s, o);
    }

    @Override
    public boolean includes(String s) {
        return false;
    }

    @Override
    public Map<String, Object> toMap() {
        return null;
    }

    @Override
    public Event overwrite(Event event) {
        return null;
    }

    @Override
    public Event append(Event event) {
        return null;
    }

    @Override
    public Object remove(String s) {
        return null;
    }

    @Override
    public String sprintf(String s) throws IOException {
        return null;
    }

    @Override
    public Event clone() {
        return null;
    }

    @Override
    public void tag(String s) {

    }
}
