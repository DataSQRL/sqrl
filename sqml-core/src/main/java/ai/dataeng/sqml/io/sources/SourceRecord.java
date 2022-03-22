package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.tree.name.Name;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.NonNull;

/**
 * Raw records of a {@link SourceTable} are represented as {@link SourceRecord}
 *
 * The generic parameter K is one of {@link java.lang.String} or {@link ai.dataeng.sqml.tree.name.Name}.
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

    public SourceRecord.Named replaceData(Map<Name,Object> newData) {
        return new SourceRecord.Named(newData, sourceTime, ingestTime, uuid);
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

    public static class Raw extends SourceRecord<String> {

        public Raw(@NonNull Map<String, Object> data, @NonNull Instant sourceTime, @NonNull Instant ingestTime, UUID uuid) {
            super(data, sourceTime, ingestTime, uuid);
        }

        public Raw(Map<String, Object> data, Instant sourceTime) {
            super(data, sourceTime);
        }
    }

    public static class Named extends SourceRecord<Name> {

        public Named(@NonNull Map<Name, Object> data, @NonNull Instant sourceTime, @NonNull Instant ingestTime, UUID uuid) {
            super(data, sourceTime, ingestTime, uuid);
        }

        public Named(Map<Name, Object> data, Instant sourceTime) {
            super(data, sourceTime);
        }
    }
}
