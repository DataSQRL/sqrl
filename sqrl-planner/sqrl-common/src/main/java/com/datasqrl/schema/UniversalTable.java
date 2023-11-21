/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import static com.datasqrl.canonicalizer.Name.changeDisplayName;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.StandardName;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.StreamUtil;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
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
    Iterator<Column> parentCols = parent.getColumns().iterator();
    for (int i = 0; i < parent.numPrimaryKeys; i++) {
      Column ppk = parentCols.next();
      addColumn(new Column(ppk.getName(), ppk.getType(), false));
    }
    this.numPrimaryKeys = parent.numPrimaryKeys + (isSingleton ? 0 : 1);
    this.path = path;
    this.name = name;
    this.hasSourceTimestamp = hasSourceTimestamp;
  }

  public List<Column> getColumns() {
    return (List) StreamUtil.filterByClass(fields.getFields(), Column.class)
        .collect(Collectors.toList());
  }

  public Stream<FieldList.IndexedField> getAllIndexedFields() {
    return fields.getIndexedFields();
  }

  public List<Field> getAllFields() {
    return fields.toList();
  }

  protected void addColumn(Column colum) {
    fields.addField(colum);
  }

  public void addColumn(final Name colName, RelDataType type) {
    addColumn(colName, type, true);
  }

  public void addColumn(Name colName, RelDataType type, boolean visible) {
    //A name may clash with a previously added name, hence we increase the version
    fields.addField(new Column(colName, type, visible));
  }

  public void addChild(Name name, UniversalTable child, Multiplicity multiplicity) {
    fields.addField(new ChildRelationship(name, child, multiplicity));
  }

  @Getter
  public static class Column extends Field {

    final RelDataType type;
    final boolean visible;

    public Column(Name name, RelDataType type, boolean visible) {
      super(name);
      this.type = type;
      this.visible = visible;
    }

    public boolean isNullable() {
      return type.isNullable();
    }
  }

  @Getter
  public static class ChildRelationship extends Field {

    final UniversalTable childTable;
    final Multiplicity multiplicity;

    public ChildRelationship(Name name, UniversalTable childTable,
        Multiplicity multiplicity) {
      super(name);
      this.childTable = childTable;
      this.multiplicity = multiplicity;
    }
  }

  public <T> List<Pair<String, T>> convert(TypeConverter<T> converter) {
    return convert(converter, true);
  }

  public <T> List<Pair<String, T>> convert(TypeConverter<T> converter, boolean onlyVisible) {
    return fields.getFields().stream()
        .filter(f -> (!onlyVisible || f.isVisible()))
        .map(f -> {
          String name = f.getId().getDisplay();
          T type;
          if (f instanceof Column) {
            Column column = (Column) f;
            type = converter.nullable(convertType(column.getType(), converter),
                column.isNullable());
          } else {
            ChildRelationship childRel = (ChildRelationship) f;
            T nestedTable = converter.nestedTable(
                childRel.getChildTable().convert(converter, onlyVisible));
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
      return TypeFactory.withNullable(typeFactory, type, nullable);
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
      tblBuilder.addColumn(ReservedName.UUID, TypeFactory.makeUuidType(typeFactory, false));
      tblBuilder.addColumn(ReservedName.INGEST_TIME, TypeFactory.makeTimestampType(typeFactory,false));
      if (hasSourceTimestamp) {
        tblBuilder.addColumn(ReservedName.SOURCE_TIME, TypeFactory.makeTimestampType(typeFactory,false));
      }
      return tblBuilder;
    }
  }

}
