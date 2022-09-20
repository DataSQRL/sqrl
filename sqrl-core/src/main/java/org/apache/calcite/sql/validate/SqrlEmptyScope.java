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

import ai.datasqrl.plan.local.generate.Resolve.StatementOp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import ai.datasqrl.schema.SQRLTable;
import java.util.Optional;
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
 * Deviant implementation of {@link SqlValidatorScope} for the top of the scope
 * stack.
 *
 * <p>It is convenient, because we never need to check whether a scope's parent
 * is null. (This scope knows not to ask about its parents, just like Adam.)
 *
 *  * SQRL:
 *  * Copy of SqlValidatorImpl.
 *  * See from git hash:
 *  *
 *  * https://github.com/DataSQRL/sqml/compare/f66cb1b3f80b6ba5295ae688be36238694d13d10...main
 */
class SqrlEmptyScope implements SqlValidatorScope {
  //~ Instance fields --------------------------------------------------------

  protected final SqrlValidatorImpl validator;
  private final Optional<StatementOp> op;

  //~ Constructors -----------------------------------------------------------

  SqrlEmptyScope(SqrlValidatorImpl validator) {
    this.validator = validator;
    this.op = Optional.empty();
  }

  SqrlEmptyScope(SqrlValidatorImpl validator, StatementOp op) {
    this.validator = validator;
    this.op = Optional.of(op);
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

  public void resolveTable(List<String> names, SqlNameMatcher nameMatcher,
      Path path, Resolved resolved) {
    final List<Resolve> imperfectResolves = new ArrayList<>();
    final List<Resolve> resolves = ((ResolvedImpl) resolved).resolves;

    //SQLR: First look for a table scope: o.entries
    Map<String, SqlValidatorNamespace> scopes = validator.getJoinScopes();
    //special case: self is also a concrete table
    if (names.get(0).equalsIgnoreCase("_") && names.size() == 1) {
      resolved.found(validator.getContextTable().get(),
          false, null, path, List.of());
      return;
    }

    if (scopes.containsKey(names.get(0)) &&
        scopes.get(names.get(0)).getTable() != null //could be in a state of unresolved (todo how to validate?)
    ) {
      resolve_rel_(validator.catalogReader.getRootSchema(), names, scopes.get(names.get(0)),
          nameMatcher, path, resolved);
      for (Resolve resolve : resolves) {
        if (resolve.remainingNames.isEmpty()) {
          // There is a full match. Return it as the only match.
          ((ResolvedImpl) resolved).clear();
          resolves.add(resolve);
          return;
        }
      }
    }
    // Look in the default schema, then default catalog, then root schema.
    for (List<String> schemaPath : validator.catalogReader.getSchemaPaths()) {
      resolve_(validator.catalogReader.getRootSchema(), names, schemaPath,
          nameMatcher, path, resolved);
      for (Resolve resolve : resolves) {
        if (resolve.remainingNames.isEmpty()) {
          // There is a full match. Return it as the only match.
          ((ResolvedImpl) resolved).clear();
          resolves.add(resolve);
          return;
        }
      }
      imperfectResolves.addAll(resolves);
    }
    // If there were no matches in the last round, restore those found in
    // previous rounds
    if (resolves.isEmpty()) {
      resolves.addAll(imperfectResolves);
    }
  }

  private void resolve_rel_(final CalciteSchema rootSchema, List<String> names,
      SqlValidatorNamespace namespace, SqlNameMatcher nameMatcher, Path path,
      Resolved resolved) {
    List<Relationship> relationships = new ArrayList<>();
    SQRLTable baseTable = null;

    SqlValidatorTable relOptTable = namespace.getTable();
    SQRLTable t = relOptTable.unwrap(SQRLTable.class);
    Preconditions.checkNotNull(t);
    CalciteSchema schema1 = new SqrlCalciteSchema((Schema)t);
    CalciteSchema schema = schema1;
    List<String> remainingNames = names;
    remainingNames = Util.skip(remainingNames); //skip alias
    if (remainingNames.size() == 0) {//no more tokens
      resolved.found(namespace, false, null, path, remainingNames);
      return;
    }

    //Need to collect all names
    TableEntry entry = null;
    int size = remainingNames.size();
    for (int i = 0; i < size; i++) {
      entry =
          schema.getTable(remainingNames.get(0), nameMatcher.isCaseSensitive());

      if (entry == null) {
        return;
      }
      if (i == 0) {
        baseTable = (SQRLTable) entry.getTable();
      }

      //Add in rels
      Relationship rel = t.getField(Name.system(remainingNames.get(0)))
          .map(f->(Relationship) f)
          .get();
      relationships.add(rel);
      t = (SQRLTable) entry.getTable();

      CalciteSchema schema3 = new SqrlCalciteSchema((Schema)entry.getTable());
      path = path.plus(null, -1, schema3.name, StructKind.NONE);
      remainingNames = Util.skip(remainingNames);
      schema = schema3;
    }

    if (entry != null && entry.getTable() instanceof Wrapper) {
      relOptTable = ((Wrapper) entry.getTable()).unwrap(PreparingTable.class);
    }
    if (entry != null) {
      final RelOptSchema relOptSchema =
          validator.catalogReader.unwrap(RelOptSchema.class);
      final RelDataType rowType = entry.getTable().getRowType(validator.typeFactory);
      relOptTable = RelOptTableImpl.create(relOptSchema, rowType, entry, null);
    }
    namespace = new RelativeTableNamespace(validator, relOptTable, baseTable, names.get(0), relationships);
    resolved.found(namespace, false, null, path, remainingNames);
  }

  private void resolve_(final CalciteSchema rootSchema, List<String> names,
      List<String> schemaNames, SqlNameMatcher nameMatcher, Path path,
      Resolved resolved) {
    final List<String> concat = ImmutableList.<String>builder()
        .addAll(schemaNames).addAll(names).build();
    CalciteSchema schema = rootSchema;
    SqlValidatorNamespace namespace = null;
    List<String> remainingNames = concat;
    int size = concat.size();
    List<Relationship> relationships = new ArrayList<>();
    SQRLTable baseTable = null;
    SQRLTable walkTable = null;
    for (int i = 0; i < size; i++) {
      String schemaName = concat.get(i);
      if (schema == rootSchema
          && nameMatcher.matches(schemaName, schema.name)) {
        remainingNames = Util.skip(remainingNames);
        continue;
      }
      final CalciteSchema subSchema =
          schema.getSubSchema(schemaName, nameMatcher.isCaseSensitive());
      if (subSchema != null) {
        path = path.plus(null, -1, subSchema.name, StructKind.NONE);
        remainingNames = Util.skip(remainingNames);
        schema = subSchema;
        namespace = new SchemaNamespace(validator,
            ImmutableList.copyOf(path.stepNames()));
        continue;
      }
      TableEntry entry =
          schema.getTable(schemaName, nameMatcher.isCaseSensitive());
      if (entry == null) {
        entry = schema.getTableBasedOnNullaryFunction(schemaName,
            nameMatcher.isCaseSensitive());
      }
      if (entry != null) {
        final Table table = entry.getTable();
        //SQRL: Allow walking a table path up until the last entry, otherwise
        // calcite can't tell when a schema ends and a table begins
        // These are absolute tables from a schema so append to a list of tables for later resolution
        if (baseTable == null) {
          baseTable = (SQRLTable) table;
          walkTable = baseTable;
        } else {
          Relationship rel = walkTable.getField(Name.system(schemaName))
              .map(f->(Relationship)f)
              .get();
          relationships.add(rel);
          walkTable = (SQRLTable)table;
        }
        if (table instanceof Schema && i != size - 1) {
          CalciteSchema schema1 = new SqrlCalciteSchema((Schema)table);
          path = path.plus(null, -1, schema1.name, StructKind.NONE);
          remainingNames = Util.skip(remainingNames);
          schema = schema1;
          namespace = new SchemaNamespace(validator,
              ImmutableList.copyOf(path.stepNames()));
          continue;
        }
        path = path.plus(null, -1, entry.name, StructKind.NONE);
        remainingNames = Util.skip(remainingNames);

        SqlValidatorTable table2 = null;
        if (table instanceof Wrapper) {
          table2 = ((Wrapper) table).unwrap(PreparingTable.class);
        }
        if (table2 == null) {
          final RelOptSchema relOptSchema =
              validator.catalogReader.unwrap(RelOptSchema.class);
          final RelDataType rowType = table.getRowType(validator.typeFactory);
          table2 = RelOptTableImpl.create(relOptSchema, rowType, entry, null);
        }

        namespace = new AbsoluteTableNamespace(validator, table2, baseTable, relationships);
        resolved.found(namespace, false, null, path, remainingNames);
        return;
      }
      // neither sub-schema nor table
      if (namespace != null
          && !remainingNames.equals(names)) {
        resolved.found(namespace, false, null, path, remainingNames);
      }
      return;
    }
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
}
