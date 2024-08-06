package com.datasqrl.plan.table;

import com.datasqrl.io.tables.TableSchema;
import java.net.URI;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

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
  public Optional<URI> getLocation() {
    return Optional.empty();
  }
}
