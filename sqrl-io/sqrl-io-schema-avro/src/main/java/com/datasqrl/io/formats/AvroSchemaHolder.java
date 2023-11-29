package com.datasqrl.io.formats;

import com.datasqrl.engine.stream.RowMapper;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.tables.SchemaValidator;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import java.net.URI;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.avro.Schema;

@Value
@AllArgsConstructor
public class AvroSchemaHolder implements TableSchema {

  Schema schema;
  String schemaDefinition;
  Optional<URI> location;


  @Override
  public RowMapper getRowMapper(RowConstructor rowConstructor,
      DataSystemConnectorSettings connectorSettings) {
    throw new NotYetImplementedException("Support for flexible schema coming soon");
  }

  @Override
  public String getSchemaType() {
    return AvroTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  public SchemaValidator getValidator(SchemaAdjustmentSettings settings,
      DataSystemConnectorSettings connectorSettings) {
    throw new NotYetImplementedException("Support for flexible schema coming soon");
  }

  @Override
  public String getDefinition() {
    return schemaDefinition;
  }
}
