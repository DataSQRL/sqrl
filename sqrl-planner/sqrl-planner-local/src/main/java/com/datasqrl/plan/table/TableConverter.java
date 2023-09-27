package com.datasqrl.plan.table;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.SchemaToUniversalTableMapperFactory;
import java.util.LinkedHashMap;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;

@AllArgsConstructor
public class TableConverter {

  TypeFactory typeFactory;
  NameCanonicalizer canonicalizer;

  public UniversalTable convert2TableBuilder(@NonNull NamePath path,
      RelDataType type, int numPrimaryKeys,
      LinkedHashMap<Integer, Name> index2Name) {
    RelDataType2UTBConverter converter = new RelDataType2UTBConverter(typeFactory, numPrimaryKeys,
        canonicalizer);
    return converter.convert(path, type, index2Name);
  }

  public RelDataType tableToDataType(UniversalTable tblBuilder, boolean includeNested,
      boolean onlyVisible) {
    return new UTB2RelDataTypeConverter(typeFactory).convertSchema(tblBuilder, includeNested, onlyVisible);
  }

  public UniversalTable sourceToTable(TableSchema tableSchema,
      DataSystemConnectorSettings connectorSettings, Optional<Name> tblAlias) {
    return SchemaToUniversalTableMapperFactory.load(tableSchema)
        .map(tableSchema, connectorSettings, tblAlias);
  }
}
