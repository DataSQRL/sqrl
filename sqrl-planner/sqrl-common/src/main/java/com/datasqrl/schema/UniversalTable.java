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
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Value
public class UniversalTable {

  Optional<UniversalTable> parent;
  RelDataType type;
  NamePath path;

  //The first n columns are form the primary key for this table
  // We make the assumption that primary key columns are always first!
  int numPrimaryKeys;

  RelDataTypeFactory typeFactory;

  Configuration configuration;


//  private UniversalTable(@NonNull Name name, @NonNull NamePath path,
//      UniversalTable parent, boolean isSingleton, boolean hasSourceTimestamp) {
//    this.parent = parent;
//    //Add parent primary key columns
//    Iterator<Column> parentCols = parent.getColumns().iterator();
//    for (int i = 0; i < parent.numPrimaryKeys; i++) {
//      Column ppk = parentCols.next();
//      addColumn(new Column(ppk.getName(), ppk.getType()));
//    }
//    this.numPrimaryKeys = parent.numPrimaryKeys + (isSingleton ? 0 : 1);
//    this.path = path;
//    this.name = name;
//    this.hasSourceTimestamp = hasSourceTimestamp;
//  }

  public static UniversalTable of(@NonNull RelDataType type, @NonNull NamePath path,
      @NonNull UniversalTable.Configuration configuration, int numPrimaryKeys, RelDataTypeFactory typeFactory) {
    Preconditions.checkArgument(path.size() > 0, "Invalid name: %s", path);
    Preconditions.checkArgument(type.isStruct() && !type.getFieldList().isEmpty(),
        "Invalid type: %s", type);
    Preconditions.checkArgument(!configuration.hasUuid || numPrimaryKeys == 1,
        "Invalid import specification: %s", numPrimaryKeys);
    RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    if (configuration.hasUuid) {
      typeBuilder.add(ReservedName.UUID, TypeFactory.makeUuidType(typeFactory, false));
    }
    if (configuration.hasIngestTime) {
      typeBuilder.add(ReservedName.INGEST_TIME, TypeFactory.makeTimestampType(typeFactory, false));
    }
    if (configuration.hasSourceTime) {
      typeBuilder.add(ReservedName.SOURCE_TIME, TypeFactory.makeTimestampType(typeFactory,false));
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
    Preconditions.checkArgument(CalciteUtil.isNestedTable(field.getType()));
    boolean isArray = CalciteUtil.isArray(field.getType());
    NamePath newPath = path.concat(Name.system(field.getName()));
    RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    //Add parent primary keys
    for (int i = 0; i < numPrimaryKeys; i++) {
      typeBuilder.add(type.getFieldList().get(i));
    }
    if (isArray && configuration.addArrayIndex) {
      typeBuilder.add(ReservedName.ARRAY_IDX, TypeFactory.makeIntegerType(typeFactory, false));
    }
    typeBuilder.addAll(field.getType().getFieldList());
    return new UniversalTable(Optional.of(this), typeBuilder.build(), newPath,
        this.numPrimaryKeys + (isArray?1:0),
        typeFactory, configuration);
  }

//
//  protected void addColumn(Column colum) {
//    fields.addField(colum);
//  }
//
//  public void addColumn(Name colName, RelDataType type) {
//    //A name may clash with a previously added name, hence we increase the version
//    fields.addField(new Column(colName, type));
//  }
//
//  public void addChild(Name name, UniversalTable child, Multiplicity multiplicity) {
//    fields.addField(new ChildRelationship(name, child, multiplicity));
//  }
//
//  @Getter
//  public static class Column extends Field {
//
//    final RelDataType type;
//
//    public Column(Name name, RelDataType type) {
//      super(name);
//      this.type = type;
//    }
//
//    public boolean isNullable() {
//      return type.isNullable();
//    }
//  }
//
//  @Getter
//  public static class ChildRelationship extends Field {
//
//    final UniversalTable childTable;
//    final Multiplicity multiplicity;
//
//    public ChildRelationship(Name name, UniversalTable childTable,
//        Multiplicity multiplicity) {
//      super(name);
//      this.childTable = childTable;
//      this.multiplicity = multiplicity;
//    }
//  }

  public interface SchemaConverter<S> {

    S convertSchema(UniversalTable tblBuilder);

  }

  @Value
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
