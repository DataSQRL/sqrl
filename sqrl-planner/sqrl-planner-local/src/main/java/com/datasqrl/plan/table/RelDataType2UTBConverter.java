/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.UniversalTable;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedHashMap;
import java.util.Optional;

@AllArgsConstructor
public class RelDataType2UTBConverter {

  public final UniversalTable.Factory tableFactory;
  public final NameCanonicalizer canonicalizer;

  public RelDataType2UTBConverter(RelDataTypeFactory typeFactory, int numPrimaryKeys,
      NameCanonicalizer canonicalizer) {
    this(new TableBuilderFactory(typeFactory, numPrimaryKeys), canonicalizer);
  }

  public UniversalTable convert(@NonNull NamePath path, RelDataType datatype,
      LinkedHashMap<Integer, Name> index2Name) {
    return createBuilder(path, null, datatype, false, index2Name);
  }

  private UniversalTable createBuilder(@NonNull NamePath path, UniversalTable parent,
      RelDataType type, boolean isSingleton,
      LinkedHashMap<Integer, Name> index2Name) {
    UniversalTable tblBuilder;
    if (parent == null) {
      tblBuilder = tableFactory.createTable(path.getLast(), path);
    } else {
      //We assume that the first field is the primary key for the nested table (and don't add an explicit IDX column
      //like we do for imports)
      tblBuilder = tableFactory.createTable(path.getLast(), path, parent, isSingleton);
    }
    //Add fields
    int index = 0;

    for (RelDataTypeField field : type.getFieldList()) {
      boolean isVisible = (index2Name == null);
      Name name = Name.of(field.getName(), canonicalizer);
      if (index2Name != null && index2Name.containsKey(index)) {
        name = index2Name.get(index);
        isVisible = true;
      }
      Optional<Pair<RelDataType, Multiplicity>> nested = getNested(field);
      if (nested.isPresent()) {
        Pair<RelDataType, Multiplicity> rel = nested.get();
        UniversalTable child = createBuilder(path.concat(name), tblBuilder,
            rel.getKey(), rel.getValue() == Multiplicity.MANY, null);
        tblBuilder.addChild(name, child, rel.getValue());
      } else {
        tblBuilder.addColumn(name, field.getType(), isVisible);
      }
      index++;
    }
    return tblBuilder;
  }

  private Optional<Pair<RelDataType, Multiplicity>> getNested(RelDataTypeField field) {
    if (CalciteUtil.isNestedTable(field.getType())) {
      Optional<RelDataType> componentType = CalciteUtil.getArrayElementType(field.getType());
      RelDataType nestedType = componentType.orElse(field.getType());
      Multiplicity multi = Multiplicity.ZERO_ONE;
      if (componentType.isPresent()) {
        multi = Multiplicity.MANY;
      } else if (!nestedType.isNullable()) {
        multi = Multiplicity.ONE;
      }
      return Optional.of(Pair.of(nestedType, multi));
    } else {
      return Optional.empty();
    }

  }

  @Value
  public static class TableBuilderFactory extends UniversalTable.AbstractFactory {

    int numPrimaryKeys;

    public TableBuilderFactory(RelDataTypeFactory typeFactory, int numPrimaryKeys) {
      super(typeFactory);
      this.numPrimaryKeys = numPrimaryKeys;
    }

    @Override
    public UniversalTable createTable(@NonNull Name name, @NonNull NamePath path) {
      return new UniversalTable(name, path, numPrimaryKeys, false);
    }
  }

}
