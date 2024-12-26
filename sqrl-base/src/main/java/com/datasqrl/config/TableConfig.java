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

    Optional<Boolean> getVirtual();
  }

  interface MetadataConfig {

    List<String> getKeys();

    Optional<MetadataEntry> getMetadataEntry(String columnName);

    Map<String, MetadataEntry> toMap();
  }

  interface ConnectorConfig {

    Map<String, Object> toMap();

    void setProperty(String key, Object value);

    //return optional<string>
    Optional<String> getFormat();

    TableType getTableType();

    Optional<String> getConnectorName();
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
    void setPartitionKey(List<String> partitionKeys);

    TableConfig build();

  }

}
