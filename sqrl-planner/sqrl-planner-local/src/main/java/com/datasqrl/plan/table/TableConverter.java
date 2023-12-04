package com.datasqrl.plan.table;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.Configuration;
import com.datasqrl.schema.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import java.util.LinkedHashMap;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

@AllArgsConstructor
public class TableConverter {

  TypeFactory typeFactory;
  NameCanonicalizer canonicalizer;

  public UniversalTable convert2TableBuilder(@NonNull NamePath path,
      RelDataType type, int numPrimaryKeys,
      LinkedHashMap<Integer, Name> index2Name) {
    return convert(path, type, numPrimaryKeys, index2Name);
  }

  private UniversalTable convert(@NonNull NamePath path, RelDataType datatype,
      int numPrimaryKeys, LinkedHashMap<Integer, Name> index2Name) {
    RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    int index=0;
    for (RelDataTypeField field : datatype.getFieldList()) {
      String fieldName = field.getName();
      if (index2Name!=null && index2Name.containsKey(index)) {
        fieldName = index2Name.get(index).getDisplay();
      }
      typeBuilder.add(fieldName,field.getType());
      index++;
    }
    return UniversalTable.of(typeBuilder.build(), path, Configuration.forTable(), numPrimaryKeys, typeFactory);
  }

  public RelDataType tableToDataType(UniversalTable tblBuilder) {
    return new UTB2RelDataTypeConverter().convertSchema(tblBuilder);
  }

  public UniversalTable sourceToTable(TableSchema tableSchema,
      boolean hasSourceTime, Name tableName, ErrorCollector errors) {
    if (tableSchema.getLocation().isPresent()) {
      errors = errors.withConfig(tableSchema.getLocation().get());
    }
    RelDataType dataType = SchemaToRelDataTypeFactory.load(tableSchema)
        .map(tableSchema, tableName, errors);
    if (dataType==null) {
      throw errors.exception(ErrorCode.SCHEMA_ERROR, "Could not convert schema for table: %s", tableName);
    }
    return UniversalTable.of(dataType, NamePath.of(tableName),
        Configuration.forImport(hasSourceTime), 1,
        typeFactory);
  }


}
