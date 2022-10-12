/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.validate;

import static org.apache.calcite.util.Static.RESOURCE;

import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import ai.datasqrl.schema.SQRLTable;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Relationship;
import com.google.common.base.Preconditions;
import org.apache.calcite.jdbc.CalciteSchema.TableEntry;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;


import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableMap;

/**
 * Deviant implementation of {@link SqlValidatorScope} for the top of the scope stack.
 *
 * <p>It is convenient, because we never need to check whether a scope's parent
 * is null. (This scope knows not to ask about its parents, just like Adam.)
 * <p>
 * * SQRL: * Copy of SqlValidatorImpl. * See from git hash: * *
 * https://github.com/DataSQRL/sqml/compare/f66cb1b3f80b6ba5295ae688be36238694d13d10...main
 */
class SqrlEmptyScope implements SqlValidatorScope {
  //~ Instance fields --------------------------------------------------------

  protected final SqrlValidatorImpl validator;

  //~ Constructors -----------------------------------------------------------

  SqrlEmptyScope(SqrlValidatorImpl validator) {
    this.validator = validator;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlValidator getValidator() {
    return validator;
  }

  public SqlQualified fullyQualify(SqlIdentifier identifier) {
    return SqlQualified.create(this, 1, null, identifier);
  }

  public SqlNode getNode() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resolve(List<String> names, SqlNameMatcher nameMatcher,
      boolean deep, Resolved resolved) {
  }

  @SuppressWarnings("deprecation")
  public SqlValidatorNamespace getTableNamespace(List<String> names) {
    SqlValidatorTable table = validator.catalogReader.getTable(names);
    return table != null
        ? new TableNamespace(validator, table)
        : null;
  }

  @Override
  public void resolveTable(List<String> names, SqlNameMatcher nameMatcher,
      Path path, Resolved resolved) {
    //    Check relative scope
    Optional<SqlValidatorNamespace> joinNs = getJoinNs(names.get(0));
    Optional<AbsoluteTableResolve> relTable = joinNs
        .map(ns -> ns.getTable())
        .map(ns -> ns.unwrap(SQRLTable.class))
        .map(t -> new AbsoluteTableResolve(
            t, t.walkField(names.subList(1, names.size()))));

    relTable.ifPresent(t -> {
      final RelOptSchema relOptSchema =
          validator.catalogReader.unwrap(RelOptSchema.class);
      if (t.getFields().isEmpty()) {
        resolved.found(validator.getContextTable().get(),
            false, null, path, List.of());
        return;
      }

      SQRLTable baseTable = (t.fields.isEmpty())
          ? t.getTable()
          : ((Relationship) t.fields.get(t.getFields().size() - 1))
              .getToTable();
      SQRLTable fromTable = (t.fields.isEmpty())
          ? t.getTable()
          : ((Relationship) t.fields.get(t.getFields().size() - 1))
              .getFromTable();

      final RelDataType rowType = baseTable.getRowType(validator.typeFactory);
      SqlValidatorTable relOptTable =
          RelOptTableImpl.create(relOptSchema, rowType,
              new SqrlCalciteSchema(fromTable)
                  .getTable(t.fields.get(t.getFields().size() - 1)
                      .getName().getCanonical(), false),
              null);
      RelativeTableNamespace namespace = new RelativeTableNamespace(validator,
          relOptTable, baseTable, names.get(0), t.fields.stream()
          .map(e -> (Relationship) e)
          .collect(Collectors.toList()));
      resolved.found(namespace, false, null, path, List.of());
    });

    //Check base schema
    Optional<SQRLTable> table = Optional.ofNullable(validator.catalogReader.getRootSchema()
            .getTable(names.get(0), false))
        .map(t -> (SQRLTable) t.getTable());
    Optional<AbsoluteTableResolve> absTable = table
        .map(t -> new AbsoluteTableResolve(t, t.walkField(names.subList(1, names.size()))));

    absTable.ifPresent(t -> {
      final RelOptSchema relOptSchema =
          validator.catalogReader.unwrap(RelOptSchema.class);

      SQRLTable baseTable = (t.fields.isEmpty())
          ? t.getTable()
          : ((Relationship) t.fields.get(t.getFields().size() - 1))
              .getToTable();

      SQRLTable fromTable = (t.fields.isEmpty())
          ? t.getTable()
          : ((Relationship) t.fields.get(t.getFields().size() - 1))
              .getFromTable();

      final RelDataType rowType = baseTable.getRowType(validator.typeFactory);
      SqlValidatorTable relOptTable = RelOptTableImpl.create(relOptSchema, rowType,
          (t.getFields().isEmpty() ? validator.catalogReader.getRootSchema()
              : new SqrlCalciteSchema(fromTable))
              .getTable(names.get(names.size() - 1), false),
          null);

      AbsoluteTableNamespace namespace = new AbsoluteTableNamespace(validator,
          relOptTable, baseTable, t.fields.stream()
          .map(e -> (Relationship) e)
          .collect(Collectors.toList()));
      resolved.found(namespace, false, null, path, List.of());
    });
  }

  private Optional<SqlValidatorNamespace> getJoinNs(String name) {
    if (name.equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      return validator.getContextTable();
    }

    Map<String, SqlValidatorNamespace> scopes = validator.getJoinScopes();
    return Optional.ofNullable(scopes.get(name));
  }

  public RelDataType nullifyType(SqlNode node, RelDataType type) {
    return type;
  }

  public void findAllColumnNames(List<SqlMoniker> result) {
  }

  public void findAllTableNames(List<SqlMoniker> result) {
  }

  public void findAliases(Collection<SqlMoniker> result) {
  }

  public RelDataType resolveColumn(String name, SqlNode ctx) {
    return null;
  }

  public SqlValidatorScope getOperandScope(SqlCall call) {
    return this;
  }

  public void validateExpr(SqlNode expr) {
    // valid
  }

  @SuppressWarnings("deprecation")
  public Pair<String, SqlValidatorNamespace> findQualifyingTableName(
      String columnName, SqlNode ctx) {
    throw validator.newValidationError(ctx,
        RESOURCE.columnNotFound(columnName));
  }

  public Map<String, ScopeChild> findQualifyingTableNames(String columnName,
      SqlNode ctx, SqlNameMatcher nameMatcher) {
    return ImmutableMap.of();
  }

  public void addChild(SqlValidatorNamespace ns, String alias,
      boolean nullable) {
    // cannot add to the empty scope
    throw new UnsupportedOperationException();
  }

  public SqlWindow lookupWindow(String name) {
    // No windows defined in this scope.
    return null;
  }

  public SqlMonotonicity getMonotonicity(SqlNode expr) {
    return
        ((expr instanceof SqlLiteral)
            || (expr instanceof SqlDynamicParam)
            || (expr instanceof SqlDataTypeSpec)) ? SqlMonotonicity.CONSTANT
            : SqlMonotonicity.NOT_MONOTONIC;
  }

  public SqlNodeList getOrderList() {
    // scope is not ordered
    return null;
  }

  @Value
  private class AbsoluteTableResolve {

    private final SQRLTable table;
    private final List<Field> fields;

  }
}
