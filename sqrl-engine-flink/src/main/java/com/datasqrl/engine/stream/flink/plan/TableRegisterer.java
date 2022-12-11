///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl.engine.stream.flink.plan;
//
//import com.datasqrl.engine.stream.flink.FlinkStreamEngine.Builder;
//import com.datasqrl.error.ErrorCollector;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.RelShuttleImpl;
//import org.apache.calcite.rel.core.TableScan;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
//import org.apache.flink.table.api.internal.FlinkEnvProxy;
//
//public class TableRegisterer extends RelShuttleImpl {
//
//  StreamTableEnvironmentImpl tEnv;
//  Builder streamBuilder;
//
//  final ErrorCollector errors = ErrorCollector.root();
//  final FlinkPhysicalPlanRewriter rewriter;
//
//  public TableRegisterer(StreamTableEnvironmentImpl tEnv, Builder streamBuilder) {
//    this.tEnv = tEnv;
//    this.streamBuilder = streamBuilder;
//    this.rewriter = new FlinkPhysicalPlanRewriter(tEnv, this);
//  }
//
//  public Table makeTable(RelNode relNode) {
//    registerSources(relNode);
//    RelNode rewritten = rewriter.rewrite(tEnv, relNode);
//    return FlinkEnvProxy.relNodeQuery(rewritten, tEnv);
//  }
//
//  private void registerSources(RelNode relNode) {
//    relNode.accept(this);
//  }
//
//  @Override
//  public RelNode visit(TableScan scan) {
//    return super.visit(scan);
//  }
//}
