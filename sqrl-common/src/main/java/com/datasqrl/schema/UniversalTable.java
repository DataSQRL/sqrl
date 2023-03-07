/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.plan.calcite.util.CalciteUtil;
import com.datasqrl.util.StreamUtil;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.name.ReservedName;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class UniversalTable {

  @Getter
  final UniversalTable parent;
  final FieldList fields = new FieldList();
  @NonNull
  final NamePath path;
  @NonNull
  final Name name;
  //The first n columns are form the primary key for this table
  // We make the assumption that primary key columns are always first!
  final int numPrimaryKeys;

  final boolean hasSourceTimestamp;

  public UniversalTable(@NonNull Name name, @NonNull NamePath path, int numPrimaryKeys,
      boolean hasSourceTimestamp) {
    this.hasSourceTimestamp = hasSourceTimestamp;
    this.parent = null;
    this.numPrimaryKeys = numPrimaryKeys;
    this.name = name;
    this.path = path;
  }

  public UniversalTable(@NonNull Name name, @NonNull NamePath path,
      UniversalTable parent, boolean isSingleton, boolean hasSourceTimestamp) {
    this.parent = parent;
    //Add parent primary key columns
    Iterator<Column> parentCols = parent.getColumns(false).iterator();
    for (int i = 0; i < parent.numPrimaryKeys; i++) {
      Column ppk = parentCols.next();
      addColumn(new Column(ppk.getName(), ppk.getVersion(), ppk.getType(), false));
    }
    this.numPrimaryKeys = parent.numPrimaryKeys + (isSingleton ? 0 : 1);
    this.path = path;
    this.name = name;
    this.hasSourceTimestamp = hasSourceTimestamp;
  }

  public List<Column> getColumns(boolean onlyVisible) {
    return (List) StreamUtil.filterByClass(fields.getFields(onlyVisible), Column.class)
        .collect(Collectors.toList());
  }

  public Stream<FieldList.IndexedField> getAllIndexedFields() {
    return fields.getIndexedFields(false);
  }

  public List<Field> getAllFields() {
    return fields.getFields(false).collect(Collectors.toList());
  }

  protected void addColumn(Column colum) {
    fields.addField(colum);
  }

  public void addColumn(final Name colName, RelDataType type) {
    addColumn(colName, type, true);
  }

  public void addColumn(final Name colName, RelDataType type, boolean visible) {
    //A name may clash with a previously added name, hence we increase the version
    int version = fields.nextVersion(colName);
    fields.addField(new Column(colName, version, type, visible));
  }

  public void addChild(Name name, UniversalTable child, Multiplicity multiplicity) {
    int version = fields.nextVersion(name);
    fields.addField(new ChildRelationship(name, version, child, multiplicity));
  }

  @Getter
  public static class Column extends Field {

    final RelDataType type;
    final boolean visible;

    public Column(Name name, int version, RelDataType type, boolean visible) {
      super(name, version);
      Preconditions.checkArgument(CalciteUtil.isBasicOrArrayType(type));
      this.type = type;
      this.visible = visible;
    }

    public boolean isNullable() {
      return type.isNullable();
    }

    @Override
    public FieldKind getKind() {
      return FieldKind.COLUMN;
    }

    @Override
    public <R, C> R accept(FieldVisitor<R, C> visitor, C context) {
      return null;
    }
  }

  @Getter
  public static class ChildRelationship extends Field {

    final UniversalTable childTable;
    final Multiplicity multiplicity;

    public ChildRelationship(Name name, int version, UniversalTable childTable,
        Multiplicity multiplicity) {
      super(name, version);
      this.childTable = childTable;
      this.multiplicity = multiplicity;
    }

    @Override
    public FieldKind getKind() {
      return FieldKind.RELATIONSHIP;
    }

    @Override
    public <R, C> R accept(FieldVisitor<R, C> visitor, C context) {
      return null;
    }
  }

  public <T> List<Pair<String, T>> convert(TypeConverter<T> converter) {
    return convert(converter, true, true);
  }

  public <T> List<Pair<String, T>> convert(TypeConverter<T> converter, boolean includeNested,
      boolean onlyVisible) {
    return fields.getFields(false)
        .filter(f -> (includeNested || (f instanceof Column)) && (!onlyVisible || f.isVisible()))
        .map(f -> {
          String name = f.getId().getCanonical();
          T type;
          if (f instanceof Column) {
            Column column = (Column) f;
            type = converter.nullable(convertType(column.getType(), converter),
                column.isNullable());
          } else {
            ChildRelationship childRel = (ChildRelationship) f;
            T nestedTable = converter.nestedTable(
                childRel.getChildTable().convert(converter, includeNested, onlyVisible));
            nestedTable = converter.nullable(nestedTable,
                childRel.multiplicity == Multiplicity.ZERO_ONE);
            if (childRel.multiplicity == Multiplicity.MANY) {
              nestedTable = converter.nullable(converter.wrapArray(nestedTable), false);
            }
            type = nestedTable;
          }
          return Pair.of(name, type);
        }).collect(Collectors.toList());
  }

  private <T> T convertType(RelDataType type, TypeConverter<T> converter) {
    Optional<RelDataType> subType = CalciteUtil.getArrayElementType(type);
    if (subType.isPresent()) {
      return converter.wrapArray(
          converter.nullable(convertType(subType.get(), converter), subType.get().isNullable()));
    } else {
      return converter.convertBasic(type);
    }
  }

  public interface TypeConverter<T> {

    T convertBasic(RelDataType type);

    T nullable(T type, boolean nullable);

    T wrapArray(T type);

    T nestedTable(List<Pair<String, T>> fields);

  }

  public interface SchemaConverter<S> {

    S convertSchema(UniversalTable tblBuilder);

  }

  public interface Factory {

    UniversalTable createTable(@NonNull Name name, @NonNull NamePath path,
        @NonNull UniversalTable parent, boolean isSingleton);

    UniversalTable createTable(@NonNull Name name, @NonNull NamePath path);

  }

  public static abstract class AbstractFactory implements Factory {

    public final RelDataTypeFactory typeFactory;

    protected AbstractFactory(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
    }

    public UniversalTable createTable(@NonNull Name name, @NonNull NamePath path,
        @NonNull UniversalTable parent, boolean isSingleton) {
      return new UniversalTable(name, path, parent, isSingleton, false);
    }

    public RelDataType withNullable(RelDataType type, boolean nullable) {
      return typeFactory.createTypeWithNullability(type, nullable);
    }

  }

  @Value
  public static class ImportFactory extends AbstractFactory {

    boolean addArrayIndex;
    boolean hasSourceTimestamp;

    public ImportFactory(RelDataTypeFactory typeFactory, boolean addArrayIndex, boolean hasSourceTimestamp) {
      super(typeFactory);
      this.addArrayIndex = addArrayIndex;
      this.hasSourceTimestamp = hasSourceTimestamp;
    }

    @Override
    public UniversalTable createTable(@NonNull Name name, @NonNull NamePath path,
        @NonNull UniversalTable parent, boolean isSingleton) {
      UniversalTable tblBuilder = super.createTable(name, path, parent, isSingleton);
      if (!isSingleton && addArrayIndex) {
        tblBuilder.addColumn(ReservedName.ARRAY_IDX,
            withNullable(typeFactory.createSqlType(SqlTypeName.INTEGER), false));
      }
      return tblBuilder;
    }

    @Override
    public UniversalTable createTable(@NonNull Name name, @NonNull NamePath path) {
      return createTable(name, path, hasSourceTimestamp);
    }

    public UniversalTable createTable(@NonNull Name name, @NonNull NamePath path,
        boolean hasSourceTimestamp) {
      UniversalTable tblBuilder = new UniversalTable(name, path, 1, hasSourceTimestamp);
      tblBuilder.addColumn(ReservedName.UUID,
          withNullable(typeFactory.createSqlType(SqlTypeName.CHAR, 36), false));
      tblBuilder.addColumn(ReservedName.INGEST_TIME,
          withNullable(typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
              false));
      if (hasSourceTimestamp) {
        tblBuilder.addColumn(ReservedName.SOURCE_TIME,
            withNullable(typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3),
                false));
      }
      return tblBuilder;
    }
  }

}
