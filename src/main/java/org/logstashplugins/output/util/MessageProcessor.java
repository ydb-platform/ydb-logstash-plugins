package org.logstashplugins.output.util;

import java.util.Map;

@FunctionalInterface
public interface MessageProcessor {
    byte[] process(Map<String, Object> data);
}