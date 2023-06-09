package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.local.generate.StatementProcessor.ProcessorContext;
import com.google.inject.Inject;
import lombok.Value;
import org.apache.calcite.sql.DistinctAssignment;
import org.apache.calcite.sql.ExportDefinition;
import org.apache.calcite.sql.ExpressionAssignment;
import org.apache.calcite.sql.ImportDefinition;
import org.apache.calcite.sql.JoinAssignment;
import org.apache.calcite.sql.QueryAssignment;
import org.apache.calcite.sql.StatementVisitor;
import org.apache.calcite.sql.StreamAssignment;

public class StatementProcessor implements StatementVisitor<Void, ProcessorContext> {

  private final ModuleLoader moduleLoader;
  private final NameCanonicalizer nameCanonicalizer;
  private final SqrlQueryPlanner planner;
  private final CalciteTableFactory tableFactory;

  @Value
  public static class ProcessorContext {
    Namespace namespace;
    ErrorCollector errors;

  }

  @Inject
  public StatementProcessor(ModuleLoader moduleLoader,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner, CalciteTableFactory tableFactory) {
    this.moduleLoader = moduleLoader;
    this.nameCanonicalizer = nameCanonicalizer;
    this.planner = planner;
    this.tableFactory = tableFactory;
  }

  @Override
  public Void visit(ImportDefinition statement, ProcessorContext ctx) {
    ImportStatementResolver importStatementResolver = new ImportStatementResolver(
        moduleLoader,
        ctx.errors, nameCanonicalizer, planner);
    importStatementResolver.resolve(statement, ctx.namespace);
    return null;
  }

  @Override
  public Void visit(ExportDefinition statement, ProcessorContext ctx) {
    ExportStatementResolver exportStatementResolver = new ExportStatementResolver(
        moduleLoader,
        ctx.errors, nameCanonicalizer, planner);
    exportStatementResolver.resolve(statement, ctx.namespace);
    return null;
  }

  @Override
  public Void visit(ExpressionAssignment statement, ProcessorContext ctx) {
    ExpressionStatementResolver expressionStatementResolver = new ExpressionStatementResolver(
        ctx.errors, nameCanonicalizer, planner);
    expressionStatementResolver.resolve(statement, ctx.namespace);
    return null;
  }

  @Override
  public Void visit(QueryAssignment statement, ProcessorContext ctx) {
    QueryStatementResolver queryStatementResolver = new QueryStatementResolver(ctx.errors,
        nameCanonicalizer, planner, tableFactory);
    queryStatementResolver.resolve(statement, ctx.namespace);
    return null;
  }

  @Override
  public Void visit(StreamAssignment statement, ProcessorContext ctx) {
    StreamStatementResolver streamStatementResolver = new StreamStatementResolver(
        ctx.errors, nameCanonicalizer, planner, tableFactory);
    streamStatementResolver.resolve(statement, ctx.namespace);
    return null;
  }

  @Override
  public Void visit(JoinAssignment statement, ProcessorContext ctx) {
    JoinStatementResolver joinStatementResolver = new JoinStatementResolver(ctx.errors,
        nameCanonicalizer, planner);
    joinStatementResolver.resolve(statement, ctx.namespace);
    return null;
  }

  @Override
  public Void visit(DistinctAssignment statement, ProcessorContext ctx) {
    DistinctStatementResolver distinctStatementResolver = new DistinctStatementResolver(
        ctx.errors, nameCanonicalizer, planner, tableFactory);
    distinctStatementResolver.resolve(statement, ctx.namespace);
    return null;
  }
}
