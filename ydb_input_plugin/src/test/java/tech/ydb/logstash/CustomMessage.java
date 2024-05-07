package tech.ydb.logstash;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import tech.ydb.topic.description.MetadataItem;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.PartitionSession;

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

    @Override
    public List<MetadataItem> getMetadataItems() {
        return Collections.emptyList();
    }
}
