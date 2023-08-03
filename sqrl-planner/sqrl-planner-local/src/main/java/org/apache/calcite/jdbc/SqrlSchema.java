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
package org.apache.calcite.jdbc;

import static java.util.Objects.requireNonNull;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.functions.SqrlFunctionCatalog;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.local.generate.TableFunctionBase;
import com.datasqrl.schema.Column;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.TypeFactory;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import com.datasqrl.plan.rules.SqrlRelMetadataQuery;
import com.datasqrl.plan.table.AbstractRelationalTable;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

@Singleton
@Getter
public class SqrlSchema extends SimpleCalciteSchema {

  private final CalciteTableFactory tableFactory;
  private final SqrlFunctionCatalog functionCatalog;
  private final ExecutionPipeline pipeline;

  private final RelOptPlanner planner;
  private final RelOptCluster cluster;
  private final RelDataTypeFactory typeFactory;

  @Inject
  public SqrlSchema(CalciteTableFactory tableFactory, SqrlFunctionCatalog functionCatalog,
      ExecutionPipeline pipeline, RelDataTypeFactory typeFactory) {
    this(CalciteSchema.createRootSchema(false, false).plus(),
        tableFactory, functionCatalog, pipeline, typeFactory);
  }

  public SqrlSchema(Schema schema, CalciteTableFactory tableFactory,
      SqrlFunctionCatalog functionCatalog, ExecutionPipeline pipeline, RelDataTypeFactory typeFactory) {
    super(null, schema, "");
    this.tableFactory = tableFactory;
    this.functionCatalog = functionCatalog;
    this.pipeline = pipeline;

    RelOptPlanner planner = new VolcanoPlanner(null, Contexts.empty());

    RelOptCluster cluster = RelOptCluster.create(
        requireNonNull(planner, "planner"),
        new RexBuilder(TypeFactory.getTypeFactory()));
    cluster.setMetadataProvider(new SqrlRelMetadataProvider());
    cluster.setMetadataQuerySupplier(SqrlRelMetadataQuery::new);
    cluster.setHintStrategies(SqrlHintStrategyTable.getHintStrategyTable());

    this.planner = planner;
    this.cluster = cluster;
    this.typeFactory = typeFactory;
  }

  public<T extends Table> Stream<T> getTableStream(Class<T> clazz) {
    return StreamUtil.filterByClass(getTableNames().stream()
        .map(name -> getTable(name, false).getTable()), clazz);
  }

  public<T extends Table> List<T> getTables(Class<T> clazz) {
    return getTableStream(clazz).collect(Collectors.toList());
  }

  public<T extends TableFunction> Stream<T> getFunctionStream(Class<T> clazz) {
    return StreamUtil.filterByClass(getFunctionNames().stream()
        .flatMap(name -> getFunctions(name, false).stream()), clazz);
  }

  public Optional<TableFunctionBase> getTableFunction(String name) {
    return StreamUtil.getOnlyElement(
        StreamUtil.filterByClass(
            getFunctions(name, false).stream(), TableFunctionBase.class));
  }

  public List<SQRLTable> getRootTables() {
    return getTables(SQRLTable.class);
  }

  public List<SQRLTable> getAllTables() {
    Set<SQRLTable> tables = new HashSet<>(getRootTables());
    Stack<SQRLTable> iter = new Stack<>();
    iter.addAll(tables);

    while (!iter.isEmpty()) {
      SQRLTable table = iter.pop();
      if (table == null) {
        continue;
      }
      List<SQRLTable> relationships = table.getFields().getAccessibleFields().stream()
          .filter(f -> f instanceof Relationship)
          .map(f -> ((Relationship) f).getToTable())
          .collect(Collectors.toList());

      for (SQRLTable rel : relationships) {
        if (!tables.contains(rel)) {
          iter.add(rel);
          tables.add(rel);
        }
      }
    }

    return tables.stream()
        .filter(f -> f != null)
        .collect(Collectors.toList());
  }

  public boolean addTable(Name name, TableSource table) {

    ScriptTableDefinition def = tableFactory.importTable(
        table, Optional.of(name) //todo can remove optional
        );

    if (getTable(name.getCanonical(), false) != null) {
      //todo: normal exception?
      throw new SqrlAstException(ErrorCode.IMPORT_NAMESPACE_CONFLICT, null,
          String.format("An item named `%s` is already in scope",
              name.getDisplay()));
    }

    registerScriptTable(def);
    return true;
  }

  public void registerScriptTable(ScriptTableDefinition tblDef) {
    for (Map.Entry<SQRLTable, VirtualRelationalTable> entry : tblDef.getShredTableMap()
        .entrySet()) {
      entry.getKey().setVT(entry.getValue());
      entry.getValue().setSqrlTable(entry.getKey());
    }
    add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());

    tblDef.getShredTableMap().values().stream().forEach(vt ->
        add(vt.getNameId(),
            vt));

    if (tblDef.getBaseTable() instanceof ProxyImportRelationalTable) {
      AbstractRelationalTable impTable = ((ProxyImportRelationalTable) tblDef.getBaseTable()).getBaseTable();
      add(impTable.getNameId(), impTable);
    }

    if (tblDef.getTable().getPath().size() == 1) {
      add(tblDef.getTable().getName().getDisplay(), (org.apache.calcite.schema.Table)
          tblDef.getTable());
    }
  }

  public void registerTableFunction(Name name, TableFunctionBase tableFunction) {
    Preconditions.checkArgument(getFunctionStream(TableFunctionBase.class)
        .noneMatch(fct -> fct.getFunctionName().equals(name)),"A function with the name %s already exists", name);
    plus().add(name.getDisplay(), tableFunction);
  }


  public <R, C> R accept(CalciteSchemaVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public void addFunction(String name, UserDefinedFunction function) {
    functionCatalog.addNativeFunction(name, function);
  }

  public Optional<List<Object>> walk(NamePath path) {
    if (path.isEmpty()) {
      return Optional.empty();
    }
    //get first table
    Optional<SQRLTable> table = Optional.ofNullable(getTable(path.get(0).getCanonical(), false))
        .filter(e -> e.getTable() instanceof SQRLTable)
        .map(e -> (SQRLTable) e.getTable());

    if (table.isEmpty()) {
      return Optional.empty();
    }

    List<Object> fields = new ArrayList<>();
    SQRLTable t = table.get();
    fields.add(t);
    Name[] names = path.popFirst().getNames();
    for (int i = 0; i < names.length; i++) {
      Optional<Field> field = t.getField(names[i]);
      if (field.isPresent()) {
        fields.add(field.get());
        if (field.get() instanceof Relationship) {
          t = ((Relationship) field.get()).getToTable();
        } else if (field.get() instanceof Column) {
          if (i != names.length - 1) {
            return Optional.empty();
          }
        }
      } else {
        return Optional.empty();
      }
    }

    return Optional.of(fields);
  }

  public Collection<Function> getFunction(String name) {
    return getFunctions(name, false);
  }
}
