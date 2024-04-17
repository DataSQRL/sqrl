package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

@AllArgsConstructor
@Getter
public class TableConfigBuilderImpl implements TableConfig.TableConfigBuilder {

  @NonNull Name name;
  @NonNull SqrlConfig config;

  public TableConfigBuilderImpl copyFrom(TableConfigImpl otherTable) {
    config.copy(otherTable.getConfig());
    return this;
  }

  public SqrlConfig getBaseConfig() {
    return config.getSubConfig(TableConfigImpl.TABLE_KEY);
  }

  public TableConfigBuilderImpl setType(ExternalDataType type) {
    getBaseConfig().setProperty(TableTableConfigImpl.TYPE_KEY, type.name().toLowerCase());
    return this;
  }

  public TableConfigBuilderImpl setWatermark(long milliseconds) {
    Preconditions.checkArgument(milliseconds >= 0, "Invalid watermark milliseconds: %s",
        milliseconds);
    getBaseConfig().setProperty(TableTableConfigImpl.WATERMARK_KEY, milliseconds);
    SqrlConfig config = getBaseConfig();
    return this;
  }

  public TableConfig.TableConfigBuilder setTimestampColumn(@NonNull String columnName) {
    getBaseConfig().setProperty(TableTableConfigImpl.TIMESTAMP_COL_KEY, columnName);
    return this;
  }

  public TableConfig.TableConfigBuilder setPrimaryKey(@NonNull String[] primaryKey) {
    getBaseConfig().setProperty(TableTableConfigImpl.PRIMARYKEY_KEY, primaryKey);
    return this;
  }

  public TableConfig.TableConfigBuilder setMetadata(@NonNull String columnName, String type, String attribute) {
    SqrlConfig colConfig = config.getSubConfig(TableConfigImpl.METADATA_KEY).getSubConfig(columnName);
    colConfig.setProperty(TableConfigImpl.METADATA_COLUMN_TYPE_KEY, type);
    colConfig.setProperty(TableConfigImpl.METADATA_COLUMN_ATTRIBUTE_KEY, attribute);
    return this;
  }

  public TableConfigBuilderImpl setMetadataFunction(@NonNull String columnName, String function) {
    SqrlConfig colConfig = config.getSubConfig(TableConfigImpl.METADATA_KEY).getSubConfig(columnName);
    colConfig.setProperty(TableConfigImpl.METADATA_COLUMN_ATTRIBUTE_KEY, function);
    return this;
  }

  public TableConfigBuilderImpl addUuid(String columnName) {
    return setMetadataFunction(columnName, "secure.Uuid()");
  }

  public TableConfigBuilderImpl addIngestTime(String columnName) {
    return setMetadataFunction(columnName, "proctime()");
  }

  public SqrlConfig getConnectorConfig() {
    return config.getSubConfig(TableConfigImpl.FLINK_CONNECTOR_KEY);
  }

  public TableConfig.TableConfigBuilder copyConnectorConfig(EngineConfigImpl connectorConfig) {
    config.getSubConfig(TableConfigImpl.FLINK_CONNECTOR_KEY)
        .copy(connectorConfig.getSqrlConfig());
    return this;
  }

  public SqrlConfig getConfig() {
    return config;
  }

  public TableConfigImpl build() {
    return new TableConfigImpl(name, config);
  }

}