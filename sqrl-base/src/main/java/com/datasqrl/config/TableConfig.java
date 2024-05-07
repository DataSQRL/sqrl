package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableType;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

public interface TableConfig {

  TableConfig load(URI uri, Name last, ErrorCollector errors);

  ConnectorConfig getConnectorConfig();

  MetadataConfig getMetadataConfig();

  TableTableConfig getBase();

  List<String> getPrimaryKeyConstraint();

  Name getName();

  TableConfigBuilder toBuilder();

  void toFile(Path tableConfigFile, boolean pretty);

  interface MetadataEntry {

    Optional<String> getType();

    Optional<String> getAttribute();
  }

  interface MetadataConfig {

    List<String> getKeys();

    Optional<MetadataEntry> getMetadataEntry(String columnName);
  }

  interface ConnectorConfig {

    Map<String, Object> toMap();

    Optional<Format> getFormat();

    TableType getTableType();

    String getConnectorName();
  }

  interface TableTableConfig {

    ExternalDataType getType();

    Optional<String> getTimestampColumn();

    long getWatermarkMillis();

    Optional<List<String>> getPartitionKey();

    Optional<List<String>> getPrimaryKey();
  }

  interface TableConfigBuilder {
    TableConfigBuilder setType(ExternalDataType externalDataType);
    TableConfigBuilder setTimestampColumn(@NonNull String columnName);
    TableConfigBuilder setWatermark(long milliseconds);
    TableConfigBuilder setMetadata(@NonNull String columnName, String type, String attribute) ;
    TableConfigBuilder setPrimaryKey(String[] pks);
    TableConfigBuilder addUuid(String columnName);
    TableConfigBuilder addIngestTime(String columnName);

    TableConfig build();
  }

  interface Format {

    String getName();

    Optional<String> getSchemaType();

    @AllArgsConstructor
    @Getter
    abstract class BaseFormat implements Format {

      @NonNull String name;

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof Format)) {
          return false;
        }
        Format that = (Format) o;
        return name.equalsIgnoreCase(that.getName());
      }

      @Override
      public int hashCode() {
        return Objects.hash(name.toLowerCase());
      }

      @Override
      public String toString() {
        return name;
      }

      @Override
      public Optional<String> getSchemaType() {
        return Optional.empty();
      }
    }

    class DefaultFormat extends BaseFormat {

      public DefaultFormat(@NonNull String name) {
        super(name);
      }

    }
  }
}
