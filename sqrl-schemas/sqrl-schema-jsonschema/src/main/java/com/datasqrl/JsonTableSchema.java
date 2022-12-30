package com.datasqrl;

import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.name.Name;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.schema.input.SchemaValidator;
import io.vertx.json.schema.Validator;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class JsonTableSchema implements TableSchema {
  Validator validator;

  @Override
  public RowMapper getRowMapper(RowConstructor rowConstructor, boolean hasSourceTimestamp) {
    return null;
  }

  @Override
  public Name getName() {
    return null;
  }

  @Override
  public SchemaValidator getValidator(TableConfig config, boolean hasSourceTimestamp) {
    return new JsonSchemaValidator(validator);
  }

  @Override
  public UniversalTable createUniversalTable(boolean hasSourceTimestamp) {

    return null;
  }
}
