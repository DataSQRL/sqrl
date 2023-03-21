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

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.functions.SqrlFunctionCatalog;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.calcite.SqrlRelBuilder;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.calcite.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.calcite.rules.SqrlRelMetadataProvider;
import com.datasqrl.plan.calcite.rules.SqrlRelMetadataQuery;
import com.datasqrl.plan.calcite.table.AbstractRelationalTable;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import com.datasqrl.plan.calcite.table.ProxyImportRelationalTable;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
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
      ExecutionPipeline pipeline) {
    this(CalciteSchema.createRootSchema(false, false).plus(), tableFactory, functionCatalog, pipeline);
  }

  public SqrlSchema(Schema schema, CalciteTableFactory tableFactory,
      SqrlFunctionCatalog functionCatalog, ExecutionPipeline pipeline) {
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
    this.typeFactory = TypeFactory.getTypeFactory();
  }

  public List<SQRLTable> getRootTables() {
    return getTableNames().stream()
        .map(name -> getTable(name, false).getTable())
        .filter(t -> t instanceof SQRLTable)
        .map(t -> (SQRLTable) t)
        .collect(Collectors.toList());
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
        table,
        Optional.of(name),//todo can remove optional
        pipeline, SqrlRelBuilder.create(getCluster(), this));

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


  public <R, C> R accept(CalciteSchemaVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public void addFunction(String name, UserDefinedFunction function) {
    functionCatalog.addNativeFunction(name, function);
  }
}
