package com.datasqrl.plan.table;

import java.nio.file.Path;
import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.io.tables.TableSchema;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class RelDataTypeTableSchema implements TableSchema {
  RelDataType relDataType;
  @Override
  public String getSchemaType() {
    return null;
  }

  @Override
  public String getDefinition() {
    return null;
  }

  @Override
  public Optional<Path> getLocation() {
    return Optional.empty();
  }
}
