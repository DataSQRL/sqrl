/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.util.ContinuousIndexMap;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.ChildRelationship;
import com.datasqrl.schema.UniversalTable.Column;
import com.datasqrl.schema.converters.SchemaToUniversalTableMapperFactory;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class CalciteTableFactory {
  @Getter
  SqrlFramework framework;

  @Getter
  private final NameCanonicalizer canonicalizer;
  private final RelDataTypeFactory typeFactory;

  @Inject
  public CalciteTableFactory(SqrlFramework framework, NameCanonicalizer nameCanonicalizer) {
    this.typeFactory = framework.getTypeFactory();
    this.framework = framework;
    this.canonicalizer = nameCanonicalizer;
  }

  private Name createTableId(@NonNull Name name) {
    return createTableId(name, null);
  }

  private Name createTableId(@NonNull Name name, String type) {
    if (!StringUtils.isEmpty(type)) {
      name = name.suffix(type);
    }
    return name.suffix(Integer.toString(framework.getUniqueTableInt().incrementAndGet()));
  }

  public static int getTableOrdinal(String tableId) {
    int idx = tableId.lastIndexOf(Name.NAME_DELIMITER);
    return Integer.parseInt(tableId.substring(idx + 1));
  }

  public ScriptTableDefinition importTable(TableSource tableSource, Optional<Name> tblAlias) {

    UniversalTable rootTable = SchemaToUniversalTableMapperFactory.load(
        tableSource.getTableSchema().get())
        .map(tableSource.getTableSchema().get(), tableSource.getConnectorSettings(), tblAlias);

    RelDataType rootType = convertTable(rootTable, true, true);
    //Currently, we only support imports through the stream engine
    ImportedRelationalTableImpl source = new ImportedRelationalTableImpl(
        createTableId(rootTable.getName(), "i"), rootType, tableSource);
    ProxyImportRelationalTable impTable = new ProxyImportRelationalTable(
        createTableId(rootTable.getName(), "q"), rootTable.getName(),
        getTimestampHolder(rootTable), rootType, source,
        TableStatistic.of(1000));

    Map<SQRLTable, VirtualRelationalTable> tables = createVirtualTables(rootTable, impTable, Optional.empty(),
        true, Optional.empty(), Optional.empty(), Optional.empty());
    return new ScriptTableDefinition(impTable, tables);
  }

  public ScriptTableDefinition defineTable(NamePath tablePath, LPAnalysis analyzedLP,
      List<Name> fieldNames, Optional<SQRLTable> parent, boolean materializeSelf,
      Optional<Supplier<RelNode>> relNodeSupplier,
      Optional<List<FunctionParameter>> parameters, Optional<List<SQRLTable>> isA) {
    ContinuousIndexMap selectMap = analyzedLP.getConvertedRelnode().getSelect();
    Preconditions.checkArgument(fieldNames.size() == selectMap.getSourceLength());

    Name tableid = createTableId(tablePath.getLast(), "q");
    ScriptRelationalTable baseTable = new QueryRelationalTable(tableid, tablePath.getLast(), analyzedLP);

    LinkedHashMap<Integer, Name> index2Name = new LinkedHashMap<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      index2Name.put(selectMap.map(i), fieldNames.get(i));
    }
    UniversalTable rootTable = convert2TableBuilder(tablePath, baseTable.getRowType(),
        baseTable.getNumPrimaryKeys(), index2Name);

    Map<SQRLTable, VirtualRelationalTable> tables = createVirtualTables(rootTable, baseTable,
        parent, materializeSelf, relNodeSupplier, parameters, isA);

    ScriptTableDefinition tblDef = new ScriptTableDefinition(baseTable, tables);
    //Currently, we do NOT preserve the order of the fields as originally defined by the user in the script.
    //This may not be an issue, but if we need to preserve the order, it is probably easiest to re-order the fields
    //of tblDef.getTable() based on the provided list of fieldNames
    return tblDef;
  }

  private static final Map<Name, Integer> defaultTimestampPreference = ImmutableMap.of(
      ReservedName.SOURCE_TIME, 20,
      ReservedName.INGEST_TIME, 3,
      Name.system("timestamp"), 10,
      Name.system("time"), 8);

  protected static int getTimestampScore(Name columnName) {
    return Optional.ofNullable(defaultTimestampPreference.get(columnName)).orElse(1);
  }

  public static Optional<Integer> getTimestampScore(Name columnName, RelDataType datatype) {
    if (!CalciteUtil.isTimestamp(datatype)) {
      return Optional.empty();
    }
    return Optional.of(getTimestampScore(columnName));
  }

  public TimestampHolder.Base getTimestampHolder(UniversalTable tblBuilder) {
    Preconditions.checkArgument(tblBuilder.getParent() == null,
        "Can only be invoked on root table");
    TimestampHolder.Base tsh = new TimestampHolder.Base();
    tblBuilder.getAllIndexedFields().forEach(indexField -> {
      if (indexField.getField() instanceof UniversalTable.Column) {
        UniversalTable.Column column = (UniversalTable.Column) indexField.getField();
        Optional<Integer> score = getTimestampScore(column.getName(), column.getType());
        score.ifPresent(s -> tsh.addCandidate(indexField.getIndex(), s));
      }
    });
    return tsh;
  }

  public UniversalTable convert2TableBuilder(@NonNull NamePath path,
      RelDataType type, int numPrimaryKeys,
      LinkedHashMap<Integer, Name> index2Name) {
    RelDataType2UTBConverter converter = new RelDataType2UTBConverter(typeFactory, numPrimaryKeys,
        canonicalizer);
    return converter.convert(path, type, index2Name);
  }

  public Map<SQRLTable, VirtualRelationalTable> createVirtualTables(UniversalTable rootTable,
      ScriptRelationalTable baseTable, Optional<SQRLTable> parent, boolean materializeSelf,
      Optional<Supplier<RelNode>> relNodeSupplier,
      Optional<List<FunctionParameter>> parameters, Optional<List<SQRLTable>> isA) {
    VirtualTableConstructor vtableBuilder = new VirtualTableConstructor(baseTable);
    Map<SQRLTable, VirtualRelationalTable> createdTables = new HashMap<>();
    build(rootTable, Optional.empty(), Optional.empty(),
        Optional.of(rootTable.getName()),null, vtableBuilder, createdTables, parent, relNodeSupplier,
        parameters, isA);
    return createdTables;
  }

  public RelDataType convertTable(UniversalTable tblBuilder, boolean includeNested,
      boolean onlyVisible) {
    return new UTB2RelDataTypeConverter().convertSchema(tblBuilder, includeNested, onlyVisible);
  }

  public class UTB2RelDataTypeConverter implements UniversalTable.TypeConverter<RelDataType>,
      UniversalTable.SchemaConverter<RelDataType> {

    @Override
    public RelDataType convertBasic(RelDataType type) {
      return type;
    }

    @Override
    public RelDataType nullable(RelDataType type, boolean nullable) {
      return typeFactory.createTypeWithNullability(type, nullable);
    }

    @Override
    public RelDataType wrapArray(RelDataType type) {

      return typeFactory.createArrayType(type, -1);
    }

    @Override
    public RelDataType nestedTable(List<Pair<String, RelDataType>> fields) {
      CalciteUtil.RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
      for (Pair<String, RelDataType> column : fields) {
        typeBuilder.add(column.getKey(), column.getRight());
      }

      return typeBuilder.build();
    }

    @Override
    public RelDataType convertSchema(UniversalTable tblBuilder) {
      return convertSchema(tblBuilder, true, true);
    }

    public RelDataType convertSchema(UniversalTable tblBuilder, boolean includeNested,
        boolean onlyVisible) {
      return nestedTable(tblBuilder.convert(this, includeNested, onlyVisible));
    }
  }

  @Getter
  private final class VirtualTableConstructor {

    ScriptRelationalTable baseTable;

    public VirtualTableConstructor(ScriptRelationalTable baseTable) {
      this.baseTable = baseTable;
    }

    public VirtualRelationalTable make(@NonNull UniversalTable tblBuilder) {
      RelDataType rowType = convertTable(tblBuilder, false, false);
      return new VirtualRelationalTable.Root(createTableId(tblBuilder.getName()), rowType, baseTable);
    }

    public VirtualRelationalTable make(@NonNull UniversalTable tblBuilder,
        VirtualRelationalTable parent, Name shredFieldName) {
      RelDataType rowType = convertTable(tblBuilder, false, false);
      return VirtualRelationalTable.Child.of(createTableId(tblBuilder.getName()), rowType, parent,
          shredFieldName.getCanonical());
    }
  }

  private void build(UniversalTable builder, Optional<SQRLTable> parent,
      Optional<VirtualRelationalTable> vParent,
      Optional<Name> fieldName,
      UniversalTable.ChildRelationship childRel,
      VirtualTableConstructor vtableBuilder,
      Map<SQRLTable, VirtualRelationalTable> createdTables, Optional<SQRLTable> sqrlParent,
      Optional<Supplier<RelNode>> relNodeSupplier,
      Optional<List<FunctionParameter>> parameters, Optional<List<SQRLTable>> isA) {
    VirtualRelationalTable vTable = vParent.map(table -> vtableBuilder.make(builder, table, childRel.getId()))
        .orElseGet(() -> vtableBuilder.make(builder));
    SQRLTable tbl;
    if (sqrlParent.isEmpty()) {
      Preconditions.checkState(builder.getPath().size() == 1);
      tbl = new RootSqrlTable(builder.getPath().getFirst(), vTable, isA.orElse(List.of()),
          parameters.orElse(List.of()),
          relNodeSupplier.orElse(
              ()->framework.getQueryPlanner().getRelBuilder().scan(vTable.getNameId()).build()));
      vTable.setSqrlTable(tbl);
      framework.getSchema().addSqrlTable(tbl);
    } else {
      tbl = new SQRLTable(builder.getPath(), vTable, List.of());
      vTable.setSqrlTable(tbl);
      framework.getSchema().addSqrlTable(tbl);
    }

    createdTables.put(tbl, vTable);
    if (sqrlParent.isPresent()) {
      //Add child relationship
      Pair<List<FunctionParameter>, SqlNode> pkWrapper = createPkWrapper((VirtualRelationalTable) sqrlParent.get().getVt(), vTable);

      Relationship relationship = new Relationship(fieldName.get(), builder.getPath(),
          framework.getUniqueColumnInt().incrementAndGet(),
          sqrlParent.get(), JoinType.CHILD,
          childRel == null ? Multiplicity.MANY : childRel.getMultiplicity(),//guess
          List.of(tbl), pkWrapper.getLeft(),
          ()->framework.getQueryPlanner().plan(Dialect.CALCITE, pkWrapper.getRight()));
      sqrlParent.get().addRelationship(relationship);
      framework.getSchema().addRelationship(relationship);
    }

    //Add all fields to proxy
    List<Field> allFields = builder.getAllFields();
    for (Field field : allFields) {
      if (field instanceof Column) {
        Column c = (Column) field;
        tbl.addColumn(c.getName(), c.isVisible(), c.getType());
      } else {
        ChildRelationship child = (ChildRelationship) field;
        build(child.getChildTable(), Optional.of(tbl),
            Optional.of(vTable), Optional.of(child.getName()), child, vtableBuilder, createdTables,
            Optional.of(tbl), Optional.empty(), Optional.empty(), Optional.empty());
      }
    }
    //Add parent relationship if not overwriting column
    if (sqrlParent.isPresent()) {
      //override field
      Relationship relationship = createParent(builder.getPath(), sqrlParent.get(),
          (VirtualRelationalTable) sqrlParent.get().getVt(), tbl, vTable);
      tbl.addRelationship(relationship);
      framework.getSchema().addRelationship(relationship);
    }
  }

  public Relationship createParent(NamePath path, SQRLTable p, VirtualRelationalTable parent, SQRLTable t, VirtualRelationalTable vTable) {
    Pair<List<FunctionParameter>, SqlNode> pkWrapper = createPkWrapper(vTable, parent);
    Relationship relationship = new Relationship(parentRelationshipName, path.concat(parentRelationshipName),
        framework.getUniqueColumnInt().incrementAndGet(),
        t, JoinType.PARENT, Multiplicity.ONE, List.of(p), pkWrapper.getLeft(),
        ()->framework.getQueryPlanner().plan(Dialect.CALCITE, pkWrapper.getRight()));
    return relationship;
  }

  public static Pair<List<FunctionParameter>, SqlNode> createPkWrapper(VirtualRelationalTable vParent, VirtualRelationalTable vTable) {
    //Parameters
    List<FunctionParameter> parameters = new ArrayList<>();
    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < Math.min(vTable.getNumPrimaryKeys(), vParent.getNumPrimaryKeys()); i++) {
      RelDataTypeField field1 = vTable.getRowType().getFieldList().get(i);
      RelDataTypeField field = vParent.getRowType().getFieldList().get(i);

      SqrlFunctionParameter param = new SqrlFunctionParameter(
          field.getName(),
          Optional.empty(),
          SqlDataTypeSpecBuilder.create(field.getType()),
          i,
          field.getType(),
          true);

      SqlDynamicParam dynamicParam = new SqlDynamicParam(i, SqlParserPos.ZERO);
      parameters.add(param);
      conditions.add(SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
          new SqlIdentifier(field1.getName(), SqlParserPos.ZERO),
          dynamicParam));
    }

    return Pair.of(parameters, new SqlSelectBuilder()
        .setFrom(new SqlIdentifier(vTable.nameId, SqlParserPos.ZERO))
        .setWhere(conditions)
        .build());
  }

  public static final Name parentRelationshipName = ReservedName.PARENT;
}
