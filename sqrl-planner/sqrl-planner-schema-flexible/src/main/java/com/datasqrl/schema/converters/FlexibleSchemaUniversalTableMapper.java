/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.Configuration;
import com.datasqrl.schema.input.FlexibleTableConverter;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.util.Optional;

@AutoService(SchemaToUniversalTableMapperFactory.class)
public class FlexibleSchemaUniversalTableMapper implements SchemaToUniversalTableMapperFactory {

  @Override
  public String getSchemaType() {
    return FlexibleTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  public UniversalTable map(TableSchema schema, DataSystemConnectorSettings connectorSettings,
      Optional<Name> tblAlias) {
    Preconditions.checkArgument(schema instanceof FlexibleTableSchemaHolder);
    FlexibleTableSchemaHolder holder = (FlexibleTableSchemaHolder)schema;
    FlexibleTableConverter converter = new FlexibleTableConverter(holder.getSchema(), tblAlias);
    return buildTable(converter, connectorSettings.isHasSourceTimestamp());
  }

  public static UniversalTable buildTable(FlexibleTableConverter converter, boolean hasSourceTime) {
    FlexibleTable2RelDataTypeConverter relDataTypeConverter = new FlexibleTable2RelDataTypeConverter();
    return UniversalTable.of(converter.apply(relDataTypeConverter), NamePath.of(converter.getName()),
        Configuration.forImport(hasSourceTime), 1,
        relDataTypeConverter.getTypeFactory());
  }
}
