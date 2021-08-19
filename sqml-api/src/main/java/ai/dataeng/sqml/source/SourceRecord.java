package ai.dataeng.sqml.source;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Raw records of a {@link SourceTable} are represented as {@link SourceRecord}
 */
public class SourceRecord implements Serializable {

    private final Map<String,Object> data;
    private final Instant sourceTime;
    private final Instant ingestTime;

    public SourceRecord(@Nonnull Map<String, Object> data, @Nonnull Instant sourceTime, @Nonnull Instant ingestTime) {
        this.data = data;
        this.sourceTime = sourceTime;
        this.ingestTime = ingestTime;
    }

    public SourceRecord(Map<String, Object> data, Instant sourceTime) {
        this(data, sourceTime, Instant.now());
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Instant getSourceTime() {
        return sourceTime;
    }

    public Instant getIngestTime() {
        return ingestTime;
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
