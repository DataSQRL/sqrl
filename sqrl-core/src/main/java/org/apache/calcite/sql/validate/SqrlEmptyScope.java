///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to you under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.calcite.sql.validate;
//
//import static org.apache.calcite.util.Static.RESOURCE;
//
//import ai.datasqrl.parse.tree.name.NamePath;
//import ai.datasqrl.parse.tree.name.ReservedName;
//import ai.datasqrl.plan.calcite.PlannerFactory;
//import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
//import ai.datasqrl.plan.local.HasToTable;
//import ai.datasqrl.schema.Field;
//import ai.datasqrl.schema.RelTypeWithHidden;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.List;
//import java.util.Map;
//
//import ai.datasqrl.schema.SQRLTable;
//import java.util.Optional;
//import java.util.UUID;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//import lombok.Getter;
//import lombok.Value;
//import org.apache.calcite.DataContext;
//import org.apache.calcite.config.CalciteConnectionConfig;
//import org.apache.calcite.linq4j.Enumerable;
//import org.apache.calcite.plan.RelOptSchema;
//import org.apache.calcite.plan.RelOptTable;
//import org.apache.calcite.prepare.Prepare.CatalogReader;
//import org.apache.calcite.prepare.RelOptTableImpl;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.core.JoinRelType;
//import org.apache.calcite.rel.type.RelDataType;
//import ai.datasqrl.schema.Relationship;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
//import org.apache.calcite.rel.type.RelDataTypeField;
//import org.apache.calcite.rel.type.StructKind;
//import org.apache.calcite.rex.RexBuilder;
//import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.schema.CustomColumnResolvingTable;
//import org.apache.calcite.schema.ScannableTable;
//import org.apache.calcite.schema.Schema.TableType;
//import org.apache.calcite.schema.Statistic;
//import org.apache.calcite.schema.Statistics;
//import org.apache.calcite.schema.Table;
//import org.apache.calcite.sql.SqlCall;
//import org.apache.calcite.sql.SqlDataTypeSpec;
//import org.apache.calcite.sql.SqlDynamicParam;
//import org.apache.calcite.sql.SqlIdentifier;
//import org.apache.calcite.sql.SqlLiteral;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.SqlNodeList;
//import org.apache.calcite.sql.SqlWindow;
//import org.apache.calcite.sql.fun.SqlStdOperatorTable;
//import org.apache.calcite.sql2rel.SqlToRelConverter;
//import org.apache.calcite.sql2rel.SqlToRelConverter.Blackboard;
//import org.apache.calcite.tools.RelBuilder;
//import org.apache.calcite.util.Pair;
//
//
//import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;
//
///**
// * Deviant implementation of {@link SqlValidatorScope} for the top of the scope stack.
// *
// * <p>It is convenient, because we never need to check whether a scope's parent
// * is null. (This scope knows not to ask about its parents, just like Adam.)
// * <p>
// * * SQRL: * Copy of SqlValidatorImpl. * See from git hash: * *
// * https://github.com/DataSQRL/sqml/compare/f66cb1b3f80b6ba5295ae688be36238694d13d10...main
// */
//public class SqrlEmptyScope implements SqlValidatorScope {
//  //~ Instance fields --------------------------------------------------------
//
//  protected final SqrlValidatorImpl validator;
//
//  //~ Constructors -----------------------------------------------------------
//
//  SqrlEmptyScope(SqrlValidatorImpl validator) {
//    this.validator = validator;
//  }
//
//  //~ Methods ----------------------------------------------------------------
//
//  public SqlValidator getValidator() {
//    return validator;
//  }
//
//  public SqlQualified fullyQualify(SqlIdentifier identifier) {
//    return SqlQualified.create(this, 1, null, identifier);
//  }
//
//  public SqlNode getNode() {
//    throw new UnsupportedOperationException();
//  }
//
//  @Override
//  public void resolve(List<String> names, SqlNameMatcher nameMatcher,
//      boolean deep, Resolved resolved) {
//    if (names.size() == 1 && names.get(0).contains(".")) {
//      resolveTable(List.of(names.get(0).split("\\.")),
//          nameMatcher, Path.EMPTY, resolved);
//    }
//
//  }
//
//  @SuppressWarnings("deprecation")
//  public SqlValidatorNamespace getTableNamespace(List<String> names) {
//    SqlValidatorTable table = validator.catalogReader.getTable(names);
//    return table != null
//        ? new TableNamespace(validator, table)
//        : null;
//  }
//
//  @Override
//  public void resolveTable(List<String> names, SqlNameMatcher nameMatcher,
//      Path path, Resolved resolved) {
//    //entries$10
//    if (resolveVirtualTable(names, nameMatcher, path, resolved)) {
//      return;
//    }
//
//    //_
//    if (resolveSelf(names, nameMatcher, path, resolved)) {
//      return;
//    }
//
//    //o.entries
//    if (resolveRelativeTable(names, nameMatcher, path, resolved)) {
//      return;
//    }
//
//    //FROM Orders.entries
//    if (resolveSchema(names, nameMatcher, path, resolved)) {
//      return;
//    }
//  }
//
//  private boolean resolveSchema(List<String> names, SqlNameMatcher nameMatcher, Path path,
//      Resolved resolved) {
//
//    Optional<SQRLTable> base = Optional.ofNullable(
//            validator.catalogReader.getRootSchema().getTable(names.get(0), false))
//        .map(t -> t.getTable())
//        .filter(t -> t instanceof SQRLTable)
//        .map(t -> (SQRLTable) t);
//
//    if (base.isEmpty()) {
//      return false;
//    } else if (names.size() == 1) {
//      resolved.found(createNs(base.get()), false, null, path, List.of());
//      return true;
//    } else {
////      if (true) {
////        return true;
////      }
//
//      Optional<SQRLTable> toTable = base.get()
//          .walkTable(NamePath.of(names.subList(1, names.size()).toArray(new String[]{})));
//      if (toTable.isEmpty()) {
//        return false;
//      }
//      List<Field> fields = base.get().walkField(names.subList(1, names.size()));
////      if (validator.forcePathIdentifiers && fields.size() > 0) {
//        //found but don't return
////        resolved.found(createAbsoluteRel(base.get(), base.get(), List.of()), false, null, path,
////            fields.stream().map(e->e.getName().getCanonical()).collect(Collectors.toList()));
////        return true;
////      }
//
//      resolved.found(createAbsoluteRel(base.get(), toTable.get(), fields), false, null, path,
//          List.of());
//      return true;
//    }
//  }
//
//  /**
//   *
//   */
//  private SqlValidatorNamespace createAbsoluteRel(SQRLTable baseTable, SQRLTable toTable,
//      List<Field> fields) {
//
//    final RelOptSchema relOptSchema =
//        validator.catalogReader.unwrap(RelOptSchema.class);
//
//    RelDataType rowType = validator.resolveNestedColumns ?
//        toTable.getRowType(validator.typeFactory)
//        : toTable.getVt().getRowType();
//
//    if (validator.forcePathIdentifiers)
//      rowType = new RelTypeWithHidden(toTable.getFullDataType(), toTable.getFullDataType().getFieldList());
//
//    SqlValidatorTable relOptTable = RelOptTableImpl.create(relOptSchema, rowType,
//        new AbsoluteSQRLTable(baseTable, toTable, fields.stream()
//            .map(e -> (Relationship) e).collect(Collectors.toList())),
//        org.apache.flink.calcite.shaded.com.google.common.collect.
//            ImmutableList.of(toTable.getVt().getNameId()));
//    TableNamespace tableNamespace = new TableNamespace(validator, relOptTable);
//    return tableNamespace;
//  }
//
//  private boolean resolveRelativeTable(List<String> names, SqlNameMatcher nameMatcher, Path path,
//      Resolved resolved) {
//    SqlValidatorNamespace joinNs = validator.getJoinScopes().get(names.get(0));
//    if (joinNs == null) {
//      return false;
//    }
//    if (names.size() == 1) {
//      return false;
//    }
//
//    List<String> remainingNames = names.subList(1, names.size());
//    SQRLTable baseTable = getBaseTable(joinNs.getTable());
//    if (baseTable == null) {
//      return false;
//    }
//    Optional<SQRLTable> toTable = baseTable
//        .walkTable(NamePath.of(remainingNames.toArray(new String[]{})));
//
//    if (toTable.isEmpty()) {
//      return false;
//    }
//
//    List<Field> fields = baseTable.walkField(remainingNames);
////    if (validator.forcePathIdentifiers && fields.size() > 0) {
////      return false;
////    }
//    resolved.found(createNestedRel(names.get(0), baseTable, toTable.get(), fields), false, null,
//        Path.EMPTY, List.of());
//    return true;
//  }
//
//  private SQRLTable getBaseTable(SqlValidatorTable table) {
//    if (table.unwrap(HasToTable.class) != null) {
//      return table.unwrap(HasToTable.class).getToTable();
//    }
//    if (table.unwrap(VirtualRelationalTable.class) != null) {
//      VirtualRelationalTable vt = table.unwrap(VirtualRelationalTable.class);
//      return vt.getSqrlTable();
//    }
//    return null;
//  }
//
//  private SqlValidatorNamespace createNestedRel(String alias, SQRLTable baseTable,
//      SQRLTable toTable,
//      List<Field> fields) {
//    final RelOptSchema relOptSchema =
//        validator.catalogReader.unwrap(RelOptSchema.class);
//
//    RelDataType rowType = toTable.getRowType(validator.typeFactory);
//    if (fields.size() > 1) {
//      rowType = addDummyPrimaryKeys(baseTable, rowType);
//    }
//    if (validator.forcePathIdentifiers) {
//      rowType = toTable.getFullDataType();
//    }
//
//
//    SqlValidatorTable relOptTable = RelOptTableImpl.create(relOptSchema, rowType,
//        new NestedSQRLTable(alias, baseTable, toTable, fields.stream()
//            .map(e -> (Relationship) e).collect(Collectors.toList())),
//        org.apache.flink.calcite.shaded.com.google.common.collect.
//            ImmutableList.of(toTable.getVt().getNameId()));
//    TableNamespace tableNamespace = new TableNamespace(validator, relOptTable);
//    return tableNamespace;
//  }
//
//  private RelDataType addDummyPrimaryKeys(SQRLTable baseTable, RelDataType rowType) {
//    List<String> pks = baseTable.getVt().getPrimaryKeyNames();
//    RelDataTypeFactory typeFactory = PlannerFactory.getTypeFactory();
//    FieldInfoBuilder b = new FieldInfoBuilder(typeFactory);
//    int i = 0;
//    for (String pk : pks) {
//      RelDataTypeField field = baseTable.getVt().getRowType().getField(pk, false, false);
//      b.add("__pk_" + (++i), field.getType());
//    }
//
//    b.addAll(rowType.getFieldList());
//
//    return b.build();
//  }
//
//  private boolean resolveSelf(List<String> names, SqlNameMatcher nameMatcher, Path path,
//      Resolved resolved) {
//    if (names.size() == 1 && names.get(0)
//        .equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
//      final List<String> assignmentPath = validator.getAssignmentPath();
//      Optional<TableResolve> resolve = Optional.ofNullable(validator.catalogReader.getRootSchema()
//              .getTable(assignmentPath.get(0), false))
//          .map(t -> (SQRLTable) t.getTable())
//          .map(t -> new TableResolve(t,
//              t.walkField(assignmentPath.subList(1, assignmentPath.size()))));
//      if (resolve.isPresent()) {
//        TableResolve t = resolve.get();
//        resolved.found(createNs(t.getToTable()), false, null, path, List.of());
//        return true;
//      }
//    }
//
//    return false;
//  }
//
//  private boolean resolveVirtualTable(List<String> names, SqlNameMatcher nameMatcher, Path path,
//      Resolved resolved) {
//    if (names.size() == 1 && names.get(0).contains("$")) {
//      resolved.found(createVt(names.get(0)), false, null, path, List.of());
//      return true;
//    }
//    return false;
//  }
//
//  private SqlValidatorNamespace createNs(SQRLTable table) {
//    final RelOptSchema relOptSchema =
//        validator.catalogReader.unwrap(RelOptSchema.class);
//
//    RelDataType rowType = table.getRowType(validator.typeFactory);
//
//    if (validator.forcePathIdentifiers)
//      rowType = table.getFullDataType();
//
//    SqlValidatorTable relOptTable = RelOptTableImpl.create(relOptSchema, rowType,
//        table,
//        org.apache.flink.calcite.shaded.com.google.common.collect.
//            ImmutableList.of(table.getVt().getNameId()));
//    TableNamespace tableNamespace = new TableNamespace(validator, relOptTable);
//    return tableNamespace;
//  }
//
//  private SqlValidatorNamespace createVt(String name) {
//    final RelOptSchema relOptSchema =
//        validator.catalogReader.unwrap(RelOptSchema.class);
//    VirtualRelationalTable virtualRelationalTable = relOptSchema.getTableForMember(
//        List.of(name)).unwrap(VirtualRelationalTable.class);
//    return createNs(virtualRelationalTable.getSqrlTable());
//  }
//
//  public RelDataType nullifyType(SqlNode node, RelDataType type) {
//    return type;
//  }
//
//  public void findAllColumnNames(List<SqlMoniker> result) {
//  }
//
//  public void findAllTableNames(List<SqlMoniker> result) {
//  }
//
//  public void findAliases(Collection<SqlMoniker> result) {
//  }
//
//  public RelDataType resolveColumn(String name, SqlNode ctx) {
//    return null;
//  }
//
//  public SqlValidatorScope getOperandScope(SqlCall call) {
//    return this;
//  }
//
//  public void validateExpr(SqlNode expr) {
//    // valid
//  }
//
//  @SuppressWarnings("deprecation")
//  public Pair<String, SqlValidatorNamespace> findQualifyingTableName(
//      String columnName, SqlNode ctx) {
//    throw validator.newValidationError(ctx,
//        RESOURCE.columnNotFound(columnName));
//  }
//
//  public Map<String, ScopeChild> findQualifyingTableNames(String columnName,
//      SqlNode ctx, SqlNameMatcher nameMatcher) {
//    return ImmutableMap.of();
//  }
//
//  public void addChild(SqlValidatorNamespace ns, String alias,
//      boolean nullable) {
//    // cannot add to the empty scope
//    throw new UnsupportedOperationException();
//  }
//
//  public SqlWindow lookupWindow(String name) {
//    // No windows defined in this scope.
//    return null;
//  }
//
//  public SqlMonotonicity getMonotonicity(SqlNode expr) {
//    return
//        ((expr instanceof SqlLiteral)
//            || (expr instanceof SqlDynamicParam)
//            || (expr instanceof SqlDataTypeSpec)) ? SqlMonotonicity.CONSTANT
//            : SqlMonotonicity.NOT_MONOTONIC;
//  }
//
//  public SqlNodeList getOrderList() {
//    // scope is not ordered
//    return null;
//  }
//
//  @Value
//  private class TableResolve {
//
//    private final SQRLTable table;
//    private final List<Field> fields;
//
//    public SQRLTable getToTable() {
//      return (fields.isEmpty())
//          ? getTable()
//          : ((Relationship) fields.get(getFields().size() - 1))
//              .getToTable();
//    }
//
//  }
//
//  @Getter
//  public class NestedSQRLTable implements Table, ScannableTable, CustomColumnResolvingTable,
//      HasToTable {
//
//    private final String alias;
//    private final SQRLTable baseTable;
//    private final SQRLTable toTable;
//    private final List<Relationship> fields;
//
//    public NestedSQRLTable(String alias, SQRLTable baseTable, SQRLTable toTable,
//        List<Relationship> fields) {
//      super();
//      this.alias = alias;
//      this.baseTable = baseTable;
//      this.toTable = toTable;
//      this.fields = fields;
//    }
//
//    @Override
//    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
//      return toTable.getVt().getRowType();
//
//    }
//
//    @Override
//    public Statistic getStatistic() {
//      return Statistics.UNKNOWN;
//    }
//
//    @Override
//    public TableType getJdbcTableType() {
//      return null;
//    }
//
//    @Override
//    public boolean isRolledUp(String s) {
//      return false;
//    }
//
//    @Override
//    public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode,
//        CalciteConnectionConfig calciteConnectionConfig) {
//      return false;
//    }
//
//    @Override
//    public Enumerable<Object[]> scan(DataContext dataContext) {
//      return null;
//    }
//
//    public RelNode toRel(SqlToRelConverter sqlToRelConverter, RelBuilder relBuilder,
//        SqlValidatorNamespace fromNamespace, CatalogReader catalogReader, String datasetName,
//        boolean[] usedDataset, Blackboard bb) {
//      RexBuilder rex = sqlToRelConverter.getRexBuilder();
//
//      for (int i = 0; i < fields.size(); i++) {
//        Relationship field = fields.get(i);
//
//        RelNode toJoin = toRelNode(field, catalogReader, sqlToRelConverter, bb);
//
//        if (i != 0) {
//          //Derive the location of PKs
//          int keys = getKeySize(field.getFromTable(), field.getToTable());
//          int totalRows = relBuilder.peek().getRowType().getFieldCount();
//          List<RexNode> pkEq = IntStream.range(0, keys)
//              .mapToObj(j -> {
//                int offset = totalRows - field.getFromTable().getVt().getRowType().getFieldCount();
//
//                RexNode lhs = rex.makeInputRef(relBuilder.peek().getRowType(), offset + j);
//                RexNode rhs = rex.makeInputRef(toJoin.getRowType(), totalRows + j);
//
//                return rex.makeCall(SqlStdOperatorTable.EQUALS, lhs, rhs);
//              })
//              .collect(Collectors.toList());
//
//          relBuilder
//              .push(toJoin)
//              //TODO: CONVERT TO LEFT (fix nullability in rel data type)
//              .join(JoinRelType.DEFAULT, pkEq);
//        } else {
//          relBuilder.push(toJoin);
//        }
//
//      }
//
//      int keys = Math.min(baseTable.getVt().getPrimaryKeyNames().size(),
//          fields.get(0).getToTable().getVt().getPrimaryKeyNames().size());
//
//      RelNode node = relBuilder.peek();
//
//      List<RexNode> pks = List.of();
//      //We need to make sure that the type is current type after transformation
//      if (fields.size() > 1) {
//        pks = IntStream.range(0, keys)
//            .mapToObj(i -> rex.makeInputRef(node, i))
//            .collect(Collectors.toList());
//      }
//
//      List<RexNode> projLast = IntStream.range(
//              node.getRowType().getFieldCount() - toTable.getVt().getRowType().getFieldCount()
//              , node.getRowType().getFieldCount())
//          .mapToObj(i -> rex.makeInputRef(relBuilder.peek(), i))
//          .collect(Collectors.toList());
//
//      List<RexNode> project = new ArrayList<>(pks);
//      project.addAll(projLast);
//
//      RelNode relNode = relBuilder
//          .project(project)
//          .build();
//      return relNode;
//    }
//
//    @Override
//    public List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType relDataType,
//        RelDataTypeFactory relDataTypeFactory, List<String> list) {
//      return toTable.resolveColumn(relDataType, relDataTypeFactory, list);
//    }
//  }
//
//  @Getter
//  public class AbsoluteSQRLTable implements Table, ScannableTable, CustomColumnResolvingTable,
//      HasToTable {
//
//    private final SQRLTable baseTable;
//    private final SQRLTable toTable;
//    private final List<Relationship> fields;
//
//    public AbsoluteSQRLTable(SQRLTable baseTable, SQRLTable toTable,
//        List<Relationship> fields) {
//      this.baseTable = baseTable;
//      this.toTable = toTable;
//      this.fields = fields;
//    }
//
//    @Override
//    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
//      return null;
//    }
//
//    @Override
//    public Statistic getStatistic() {
//      return Statistics.UNKNOWN;
//    }
//
//    @Override
//    public TableType getJdbcTableType() {
//      return null;
//    }
//
//    @Override
//    public boolean isRolledUp(String s) {
//      return false;
//    }
//
//    @Override
//    public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode,
//        CalciteConnectionConfig calciteConnectionConfig) {
//      return false;
//    }
//
//    @Override
//    public Enumerable<Object[]> scan(DataContext dataContext) {
//      return null;
//    }
//
//    public RelNode toRel(SqlToRelConverter sqlToRelConverter, RelBuilder relBuilder,
//        SqlValidatorNamespace fromNamespace, CatalogReader catalogReader, String datasetName,
//        boolean[] usedDataset, Blackboard bb) {
//      RexBuilder rex = sqlToRelConverter.getRexBuilder();
//
//      //Push in the first, join remaining w/ pks
//      RelOptTable t = getRelOptTable(baseTable, catalogReader);
//      RelNode scan = sqlToRelConverter.toRel(t, List.of());
//      relBuilder.push(scan);
//
//      for (int i = 0; i < fields.size(); i++) {
//        Relationship field = fields.get(i);
//        RelNode toJoin = toRelNode(field, catalogReader, sqlToRelConverter, bb);
//
//        //Derive the location of PKs
//        int keys = getKeySize(field.getFromTable(), field.getToTable());
//        int totalRows = relBuilder.peek().getRowType().getFieldCount();
//        List<RexNode> pkEq = IntStream.range(0, keys)
//            .mapToObj(j -> {
//              int offset = totalRows - field.getFromTable().getVt().getRowType().getFieldCount();
//
//              RexNode lhs = rex.makeInputRef(relBuilder.peek().getRowType(), offset + j);
//              RexNode rhs = rex.makeInputRef(toJoin.getRowType(), totalRows + j);
//
//              return rex.makeCall(SqlStdOperatorTable.EQUALS, lhs, rhs);
//            })
//            .collect(Collectors.toList());
//
//        relBuilder
//            .push(toJoin)
//            //TODO: CONVERT TO LEFT (fix nullability in rel data type)
//            .join(JoinRelType.DEFAULT, pkEq);
//      }
//
//      int keys = Math.min(baseTable.getVt().getPrimaryKeyNames().size(),
//          fields.get(0).getToTable().getVt().getPrimaryKeyNames().size());
//
//      RelNode node = relBuilder.peek();
//
//      List<RexNode> projLast = IntStream.range(
//              node.getRowType().getFieldCount() - toTable.getVt().getRowType().getFieldCount()
//              , node.getRowType().getFieldCount())
//          .mapToObj(i -> rex.makeInputRef(relBuilder.peek(), i))
//          .collect(Collectors.toList());
//
//      RelNode relNode = relBuilder
//          .project(projLast)
//          .build();
//      return relNode;
//    }
//
//    @Override
//    public List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType relDataType,
//        RelDataTypeFactory relDataTypeFactory, List<String> list) {
//      return toTable.resolveColumn(relDataType, relDataTypeFactory, list);
//    }
//  }
//
//  private RelNode toRelNode(Relationship field, CatalogReader catalogReader,
//      SqlToRelConverter sqlToRelConverter, Blackboard bb) {
//
//    if (field.getNode() != null) {
//      validator.validate(field.getNode());
//      sqlToRelConverter.convertFrom(bb, field.getNode());
//      return bb.root;
//    } else {
//      RelOptTable relOptTable = getRelOptTable(field.getToTable(), catalogReader);
//      RelNode toJoin = sqlToRelConverter.toRel(relOptTable, List.of());
//      return toJoin;
//    }
//  }
//
//  private int getKeySize(SQRLTable fromTable, SQRLTable toTable) {
//    return Math.min(fromTable.getVt().getPrimaryKeyNames().size(),
//        toTable.getVt().getPrimaryKeyNames().size());
//  }
//
//  private RelOptTable getRelOptTable(SQRLTable sqrl,
//      CatalogReader catalogReader) {
//    return catalogReader.getTableForMember(List.of(sqrl.getVt().getNameId()));
////      final RelOptSchema relOptSchema =
////          validator.catalogReader.unwrap(RelOptSchema.class);
////
////      RelOptTable t = RelOptTableImpl.create(relOptSchema, table.getVt().getRowType(),
////          table,
////          org.apache.flink.calcite.shaded.com.google.common.collect.
////              ImmutableList.of(table.getVt().getNameId()));
////      return t;
//  }
//
//}
