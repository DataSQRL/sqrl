package com.datasqrl.io;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import lombok.*;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Raw records of a {@link TableSource} are represented as {@link SourceRecord}
 * <p>
 * The generic parameter K is one of {@link java.lang.String} or {@link Name}.
 */
@NoArgsConstructor
@Getter
@Setter
public class SourceRecord<K> implements Serializable {

  private Map<K, Object> data;
  private Instant sourceTime;
  private Instant ingestTime;
  private UUID uuid;

  public SourceRecord(@NonNull Map<K, Object> data, Instant sourceTime, @NonNull Instant ingestTime,
      UUID uuid) {
    this.data = data;
    this.sourceTime = sourceTime;
    this.ingestTime = ingestTime;
    this.uuid = uuid;
  }

  public SourceRecord(Map<K, Object> data, Instant sourceTime) {
    this(data, sourceTime, Instant.now(), makeUUID());
  }

  public static UUID makeUUID() {
    return UUID.randomUUID();
  }

  public boolean hasUUID() {
    return uuid != null;
  }

  public SourceRecord.Named replaceData(Map<Name, Object> newData) {
    return new SourceRecord.Named(newData, sourceTime, ingestTime, uuid);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceRecord that = (SourceRecord) o;
    return data.equals(that.data) && sourceTime.equals(that.sourceTime) && ingestTime.equals(
        that.ingestTime);
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

  @NoArgsConstructor
  public static class Raw extends SourceRecord<String> {

    public Raw(@NonNull Map<String, Object> data, Instant sourceTime, @NonNull Instant ingestTime,
        UUID uuid) {
      super(data, sourceTime, ingestTime, uuid);
    }

    public Raw(Map<String, Object> data, Instant sourceTime) {
      super(data, sourceTime);
    }
  }

  @NoArgsConstructor
  public static class Named extends SourceRecord<Name> {

    public Named(@NonNull Map<Name, Object> data, Instant sourceTime, @NonNull Instant ingestTime,
        UUID uuid) {
      super(data, sourceTime, ingestTime, uuid);
    }

    public Named(Map<Name, Object> data, Instant sourceTime) {
      super(data, sourceTime);
    }
  }
}
