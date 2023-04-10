package com.datasqrl;

import com.datasqrl.io.tables.SchemaDefinition;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.name.Name;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.datasqrl.schema.input.SchemaValidator;
import io.vertx.json.schema.Validator;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class JsonTableSchema implements TableSchema {

  public static final String SCHEMA_TYPE = "json";

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
  public String getSchemaType() {
    return SCHEMA_TYPE;
  }

  @Override
  public SchemaValidator getValidator(SchemaAdjustmentSettings adjustmentSettings, boolean hasSourceTimestamp) {
    return new JsonSchemaValidator(validator);
  }

  @Override
  public UniversalTable createUniversalTable(boolean hasSourceTimestamp, Optional<Name> tblAlias) {
    throw new RuntimeException("Not yet implemented");
  }

  @Override
  public SchemaDefinition getDefinition() {
    return null;
  }
}
