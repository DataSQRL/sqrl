package ai.dataeng.sqml.source;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Raw records of a {@link SourceTable} are represented as {@link SourceRecord}
 */
public class SourceRecord implements Serializable {

    private final Map<String,Object> data;
    private final OffsetDateTime sourceTime;
    private final OffsetDateTime ingestTime;

    public SourceRecord(@Nonnull Map<String, Object> data, @Nonnull OffsetDateTime sourceTime, @Nonnull OffsetDateTime ingestTime) {
        this.data = data;
        this.sourceTime = sourceTime;
        this.ingestTime = ingestTime;
    }

    public SourceRecord(Map<String, Object> data, OffsetDateTime sourceTime) {
        this(data, sourceTime, OffsetDateTime.now());
    }

    public Map<String, Object> getData() {
        return data;
    }

    public OffsetDateTime getSourceTime() {
        return sourceTime;
    }

    public OffsetDateTime getIngestTime() {
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
