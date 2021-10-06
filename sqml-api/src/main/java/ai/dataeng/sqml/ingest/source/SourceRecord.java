package ai.dataeng.sqml.ingest.source;

import lombok.NonNull;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Raw records of a {@link SourceTable} are represented as {@link SourceRecord}
 */
public class SourceRecord<K> implements Serializable {

    private final Map<K,Object> data;
    private final Instant sourceTime;
    private final Instant ingestTime;
    private final UUID uuid;

    public SourceRecord(@NonNull Map<K, Object> data, @NonNull Instant sourceTime, @NonNull Instant ingestTime, UUID uuid) {
        this.data = data;
        this.sourceTime = sourceTime;
        this.ingestTime = ingestTime;
        this.uuid = uuid;
    }

    public SourceRecord(Map<K, Object> data, Instant sourceTime) {
        this(data, sourceTime, Instant.now(), UUID.randomUUID());
    }

    public Map<K, Object> getData() {
        return data;
    }

    public Instant getSourceTime() {
        return sourceTime;
    }

    public Instant getIngestTime() {
        return ingestTime;
    }

    public boolean hasUUID() {
        return uuid!=null;
    }

    public UUID getUuid() {
        return uuid;
    }

    public<T> SourceRecord<T> replaceData(Map<T,Object> newData) {
        return new SourceRecord<>(newData, sourceTime, ingestTime, uuid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SourceRecord that = (SourceRecord) o;
        return data.equals(that.data) && sourceTime.equals(that.sourceTime) && ingestTime.equals(that.ingestTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, sourceTime, ingestTime);
    }

    @Override
    public String toString() {
        return "SourceRecord{" +
                "data=" + data +
                ", sourceTime=" + sourceTime +
                ", ingestTime=" + ingestTime +
                '}';
    }
}
