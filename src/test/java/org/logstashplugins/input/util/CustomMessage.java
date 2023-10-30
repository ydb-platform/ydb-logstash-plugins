package org.logstashplugins.input.util;

import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.PartitionSession;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CustomMessage implements Message {

    private final byte[] data;
    private final long offset;
    private final long seqNo;

    public CustomMessage(byte[] data, long offset, long seqNo) {
        this.data = data;
        this.offset = offset;
        this.seqNo = seqNo;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public long getSeqNo() {
        return seqNo;
    }

    @Override
    public Instant getCreatedAt() {
        return null;
    }

    @Override
    public String getMessageGroupId() {
        return null;
    }

    @Override
    public String getProducerId() {
        return null;
    }

    @Override
    public Map<String, String> getWriteSessionMeta() {
        return null;
    }

    @Override
    public Instant getWrittenAt() {
        return null;
    }

    @Override
    public PartitionSession getPartitionSession() {
        return null;
    }

    @Override
    public CompletableFuture<Void> commit() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);
        return future;
    }
}
