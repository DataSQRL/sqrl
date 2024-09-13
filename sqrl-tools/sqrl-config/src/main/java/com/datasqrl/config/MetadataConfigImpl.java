package com.datasqrl.config;

import com.datasqrl.config.TableConfig.MetadataEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MetadataConfigImpl implements TableConfig.MetadataConfig {
  SqrlConfig sqrlConfig;

  @Override
  public List<String> getKeys() {
    return StreamSupport.stream(sqrlConfig.getKeys().spliterator(), false)
        .collect(Collectors.toList());
  }

  @Override
  public Optional<TableConfig.MetadataEntry> getMetadataEntry(String columnName) {
//    if (!sqrlConfig.hasKey(columnName)) return Optional.empty();//todo: hasKey and containsKey does not work with subconfigs :(

    return Optional.of(new MetadataEntryImpl(sqrlConfig.getSubConfig(columnName)));
  }

  @Override
  public Map<String, MetadataEntry> toMap() {
    return StreamSupport.stream(sqrlConfig.getKeys().spliterator(), false)
        .collect(Collectors.toMap(c->c, c->new MetadataEntryImpl(sqrlConfig.getSubConfig(c))));
  }
}
