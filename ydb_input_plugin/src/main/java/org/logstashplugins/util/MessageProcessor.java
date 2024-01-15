package org.logstashplugins.util;

import tech.ydb.topic.read.Message;

import java.util.Map;
import java.util.function.Consumer;

@FunctionalInterface
public interface MessageProcessor {
    Map<String, Object> process(Message message, Consumer<Map<String, Object>> consumer);
}
