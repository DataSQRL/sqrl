/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
//package com.datasqrl.plan.calcite.memory;
//
//import com.datasqrl.environment.ImportManager.SourceTableImport;
//import com.datasqrl.io.util.StreamInputPreparer;
//import com.datasqrl.physical.stream.StreamEngine;
//import com.datasqrl.physical.stream.inmemory.InMemStreamEngine;
//import com.datasqrl.plan.calcite.BasicSqrlCalciteBridge;
//import com.datasqrl.plan.calcite.OptimizationStage;
//import com.datasqrl.plan.calcite.Planner;
//import com.datasqrl.plan.calcite.memory.table.DataTable;
//import com.datasqrl.plan.calcite.sqrl.table.*;
//import lombok.Getter;
//import lombok.SneakyThrows;
//import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
//import org.apache.calcite.adapter.enumerable.EnumerableRel;
//import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
//import org.apache.calcite.jdbc.CalciteSchema;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.type.RelDataTypeField;
//import org.apache.calcite.runtime.Bindable;
//import org.apache.calcite.schema.SchemaPlus;
//import org.apache.calcite.tools.RelBuilder;
//
//import java.util.*;
//
///**
// * The {@link EnumerableCalciteBridge} plans and immediately executes all {@link com.datasqrl.plan.local.operations.SchemaUpdateOp}
// * that it visits. It stores the results of that execution as a {@link DataTable} inside the {@link InMemoryCalciteSchema}
// * that it maintains separately from tableMap of the parent class, because that schema is used to initiate the
// * {@link LocalDataContext} required for the enumerable execution by Calcite.
// *
// * This bridge implementation is used primarily for testing and in-memory execution since it eagerly materializes
// * all defined SQRL tables and stores the results in memory.
// *
// */
//public class EnumerableCalciteBridge extends BasicSqrlCalciteBridge {
//
//  private final StreamEngine streamEngine;
//  private final StreamInputPreparer streamPreparer;
//
//  @Getter
//  private final InMemoryCalciteSchema inMemorySchema;
//
//  public EnumerableCalciteBridge(Planner planner, CalciteTableFactory tableFactory,
//                                 StreamEngine streamEngine, StreamInputPreparer streamPreparer) {
//    super(planner, tableFactory);
//    this.streamEngine = streamEngine;
//    this.streamPreparer = streamPreparer;
//    this.inMemorySchema = new InMemoryCalciteSchema();
//  }
//
//  /**
//   */
//  @Override
//  @SneakyThrows
//  public <T> T visit(SourceTableImportOp op) {
//    List<AbstractRelationalTable> createdTables = super.visit(op);
//
//    ProxyImportRelationalTable impTable = (ProxyImportRelationalTable) createdTables.get(0);
//    setData(impTable.getNameId(), impTable.getRowType().getFieldList(), getDataFromImport(impTable.getSourceTableImport()));
//
//    QueryRelationalTable queryTable = (QueryRelationalTable) createdTables.get(1);
//    executeAndRegister(queryTable.getNameId());
//    return null;
//  }
//
//  public Collection<Object[]> getDataFromImport(SourceTableImport tableImport) {
//    StreamEngine.Builder streamJobBuilder = streamEngine.createJob();
//    streamPreparer.importTable(tableImport,streamJobBuilder);
//    StreamEngine.Job job = streamJobBuilder.build();
//    String tableQualifiedName = tableImport.getTable().qualifiedName();
//    job.execute("populate["+tableQualifiedName+"]");
//
//    Collection<Object[]> objects = ((InMemStreamEngine.Job)job).getRecordHolder().get(tableQualifiedName);
//    return objects;
//  }
//
//  @SneakyThrows
//  @Override
//  public <T> T visit(AddColumnOp op) {
//    super.visit(op);
//    executeAndRegister(op.getTable().getId().getCanonical());
//    return null;
//  }
//
//  @SneakyThrows
//  @Override
//  public <T> T visit(AddNestedTableOp op) {
//    super.visit(op);
//    executeAndRegister(op.getTable().getId().getCanonical());
//    return super.visit(op);
//  }
//
//  @SneakyThrows
//  @Override
//  public <T> T visit(AddRootTableOp op) {
//    super.visit(op);
//    executeAndRegister(op.getTable().getId().getCanonical());
//
//    return super.visit(op);
//  }
//
//  @SneakyThrows
//  public void executeAndRegister(String tableName) {
//    RelBuilder builder = planner.getRelBuilder();
//
//    RelNode rel = builder
//        .scan(tableName)
//        .build();
//
//    List<Object[]> data = execute(rel);
//
//    setData(tableName, rel.getRowType().getFieldList(), data);
//  }
//
//  private void setData(String name, List<RelDataTypeField> header, Collection<Object[]> data) {
//    DataTable dataTable = new DataTable(header, data);
//    this.inMemorySchema.registerDataTable(name, dataTable);
//    this.tableMap.put(name, dataTable);
//
//  }
//
//  @SneakyThrows
//  public List<Object[]> execute(RelNode node) {
//    System.out.println("-Before Optimization:\n" + node.explain());
//    for (OptimizationStage stage : OptimizationStage.ENUMERABLE_STAGES) {
//      node = planner.transform(stage,node);
//      System.out.println("-Stage "+stage.getName()+":\n"+node.explain());
//    }
//
//
//    Bindable<Object[]> bindable = EnumerableInterpretable.toBindable(new HashMap<>(),
//        null, (EnumerableRel) node, Prefer.ARRAY);
//
//    List<Object[]> results = new ArrayList<>();
//
//    SchemaPlus rootMemSchema = CalciteSchema.createRootSchema(false, false).plus();
//    rootMemSchema.add(planner.getDefaultSchema().getName(), inMemorySchema);
//
//    LocalDataContext ctx = new LocalDataContext(rootMemSchema);
//    System.out.println("--Results");
//    for (Object o : bindable.bind(ctx)) {
//      if (o instanceof Object[]) {
//        results.add((Object[]) o);
//        System.out.println(Arrays.toString((Object[]) o));
//      } else {
//        results.add(new Object[]{o});
//      }
//    }
//    return results;
//  }
//}
