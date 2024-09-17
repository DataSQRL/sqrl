package com.datasqrl.config;

import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MetadataEntryImpl implements TableConfig.MetadataEntry {
  SqrlConfig sqrlConfig;
  @Override
  public Optional<String> getType() {
    SqrlConfig.Value<String> type = sqrlConfig.asString(TableConfigImpl.METADATA_COLUMN_TYPE_KEY);

    return type.getOptional();
  }

  @Override
  public Optional<String> getAttribute() {
    return sqrlConfig.asString(TableConfigImpl.METADATA_COLUMN_ATTRIBUTE_KEY).getOptional();
  }

  @Override
  public Optional<Boolean> getVirtual() {
    return sqrlConfig.asBool(TableConfigImpl.METADATA_VIRTUAL_ATTRIBUTE_KEY).getOptional();
  }
}
