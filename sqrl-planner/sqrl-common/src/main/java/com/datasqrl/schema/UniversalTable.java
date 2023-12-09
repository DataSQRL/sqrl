/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
public class UniversalTable {

  Optional<UniversalTable> parent;
  RelDataType type;
  NamePath path;

  //The first n columns are form the primary key for this table
  // We make the assumption that primary key columns are always first!
  int numPrimaryKeys;

  RelDataTypeFactory typeFactory;

  Configuration configuration;

  public static UniversalTable of(@NonNull RelDataType type, @NonNull NamePath path,
      @NonNull UniversalTable.Configuration configuration, int numPrimaryKeys, RelDataTypeFactory typeFactory) {
    Preconditions.checkArgument(path.size() > 0, "Invalid name: %s", path);
    Preconditions.checkArgument(type.isStruct() && !type.getFieldList().isEmpty(),
        "Invalid type: %s", type);
    Preconditions.checkArgument(!configuration.hasUuid || numPrimaryKeys == 1,
        "Invalid import specification: %s", numPrimaryKeys);
    RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    NameAdjuster nameAdjuster = new NameAdjuster(type.getFieldNames());
    if (configuration.hasUuid) {
      typeBuilder.add(nameAdjuster.uniquifyName(ReservedName.UUID), TypeFactory.makeUuidType(typeFactory, false));
    }
    if (configuration.hasIngestTime) {
      typeBuilder.add(nameAdjuster.uniquifyName(ReservedName.INGEST_TIME), TypeFactory.makeTimestampType(typeFactory, false));
    }
    if (configuration.hasSourceTime) {
      typeBuilder.add(nameAdjuster.uniquifyName(ReservedName.SOURCE_TIME), TypeFactory.makeTimestampType(typeFactory,false));
    }
    typeBuilder.addAll(type.getFieldList());
    return new UniversalTable(Optional.empty(), typeBuilder.build(), path, numPrimaryKeys, typeFactory, configuration);
  }

  public Name getName() {
    return path.getLast();
  }

  public List<RelDataTypeField> getAllIndexedFields() {
    return getAllFields();
  }

  public List<RelDataTypeField> getAllFields() {
    return type.getFieldList();
  }

  public Map<String, UniversalTable> getNestedTables() {
    return type.getFieldList().stream().filter(f -> CalciteUtil.isNestedTable(f.getType()))
        .collect(Collectors.toMap(RelDataTypeField::getName, this::createChild));
  }

  private UniversalTable createChild(RelDataTypeField field) {
    RelDataType nestedType = CalciteUtil.getNestedTableType(field.getType()).get();
    boolean isArray = CalciteUtil.isArray(field.getType());
    NamePath newPath = path.concat(Name.system(field.getName()));
    RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    NameAdjuster nameAdjuster = new NameAdjuster(nestedType.getFieldNames());
    //Add parent primary keys
    for (int i = 0; i < numPrimaryKeys; i++) {
      RelDataTypeField pkField = type.getFieldList().get(i);
      typeBuilder.add(nameAdjuster.uniquifyName(pkField.getName()), pkField.getType());
    }
    if (isArray && configuration.addArrayIndex) {
      typeBuilder.add(nameAdjuster.uniquifyName(ReservedName.ARRAY_IDX), TypeFactory.makeIntegerType(typeFactory, false));
    }
    typeBuilder.addAll(nestedType.getFieldList());
    return new UniversalTable(Optional.of(this), typeBuilder.build(), newPath,
        this.numPrimaryKeys + (isArray?1:0),
        typeFactory, configuration);
  }

  /**
   * NameAdjuster makes sure that any additional columns we add to a table (e.g. primary keys or timestamps) are unique and do
   * not clash with existing columns by `uniquifying` them using Calcite's standard way of doing this.
   * Because we want to preserve the names of the user-defined columns and primary key columns are added first, we have to use
   * this custom way of uniquifying column names.
   */
  public static class NameAdjuster {

    Set<String> names;

    public NameAdjuster(Collection<String> names) {
      this.names = new HashSet<>(names);
      Preconditions.checkArgument(this.names.size() == names.size(), "Duplicate names in set of columns: %s", names);
    }

    public String uniquifyName(Name name) {
      return uniquifyName(name.getDisplay());
    }

    public String uniquifyName(String name) {
      String uniqueName = SqlValidatorUtil.uniquify(
          name,
          names,
          SqlValidatorUtil.EXPR_SUGGESTER);
      names.add(uniqueName);
      return uniqueName;
    }

  }

  public interface SchemaConverter<S> {

    S convertSchema(UniversalTable tblBuilder);

  }

  @AllArgsConstructor
  public static class Configuration {

    boolean hasUuid;
    boolean hasIngestTime;
    boolean hasSourceTime;
    boolean addArrayIndex;

    public static Configuration forImport(boolean hasSourceTime) {
      return new Configuration(true, true, hasSourceTime, false);
    }

    public static Configuration forTable() {
      return new Configuration(false, false, false, false);
    }
  }
}
