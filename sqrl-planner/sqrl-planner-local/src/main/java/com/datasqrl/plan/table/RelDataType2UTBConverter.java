/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.UniversalTable.Configuration;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.util.RelDataTypeBuilder;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashMap;
import java.util.Optional;

@Value
public class RelDataType2UTBConverter {

  public final RelDataTypeFactory typeFactory;


  public UniversalTable convert(@NonNull NamePath path, RelDataType datatype,
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


}
