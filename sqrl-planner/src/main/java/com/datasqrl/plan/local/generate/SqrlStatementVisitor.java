package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.name.NameCanonicalizer;
import lombok.Builder;
import lombok.Value;
import org.apache.calcite.sql.DistinctAssignment;
import org.apache.calcite.sql.ExportDefinition;
import org.apache.calcite.sql.ExpressionAssignment;
import org.apache.calcite.sql.ImportDefinition;
import org.apache.calcite.sql.JoinAssignment;
import org.apache.calcite.sql.QueryAssignment;
import org.apache.calcite.sql.StatementVisitor;
import org.apache.calcite.sql.StreamAssignment;

public class SqrlStatementVisitor implements StatementVisitor<Void, FlinkNamespace> {

  private final SystemContext systemContext;

  @Value
  @Builder
  public static class SystemContext {
    NameCanonicalizer nameCanonicalizer;
    ErrorCollector errors;
    ModuleLoader moduleLoader;
  }
  public SqrlStatementVisitor(SystemContext systemContext) {
    this.systemContext = systemContext;
  }

  @Override
  public Void visit(ImportDefinition statement, FlinkNamespace ns) {
    ImportStatementResolver importStatementResolver = new ImportStatementResolver(systemContext);
    importStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(ExportDefinition statement, FlinkNamespace ns) {
    ExportStatementResolver exportStatementResolver = new ExportStatementResolver(systemContext);
    exportStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(StreamAssignment statement, FlinkNamespace ns) {
    StreamStatementResolver streamStatementResolver = new StreamStatementResolver(systemContext);
    streamStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(ExpressionAssignment statement, FlinkNamespace ns) {
    ExpressionStatementResolver expressionStatementResolver = new ExpressionStatementResolver(systemContext);
    expressionStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(QueryAssignment statement, FlinkNamespace ns) {
    QueryStatementResolver queryStatementResolver = new QueryStatementResolver(systemContext);
    queryStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(JoinAssignment statement, FlinkNamespace ns) {
    JoinStatementResolver joinStatementResolver = new JoinStatementResolver(systemContext);
    joinStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(DistinctAssignment statement, FlinkNamespace ns) {
    DistinctStatementResolver distinctStatementResolver = new DistinctStatementResolver(systemContext);
    distinctStatementResolver.resolve(statement, ns);
    return null;
  }
}
