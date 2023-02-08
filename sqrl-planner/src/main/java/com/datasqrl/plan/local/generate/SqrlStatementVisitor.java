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

public class SqrlStatementVisitor implements StatementVisitor<Void, Namespace> {

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
  public Void visit(ImportDefinition statement, Namespace ns) {
    ImportStatementResolver importStatementResolver = new ImportStatementResolver(systemContext);
    importStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(ExportDefinition statement, Namespace ns) {
    ExportStatementResolver exportStatementResolver = new ExportStatementResolver(systemContext);
    exportStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(StreamAssignment statement, Namespace ns) {
    StreamStatementResolver streamStatementResolver = new StreamStatementResolver(systemContext);
    streamStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(ExpressionAssignment statement, Namespace ns) {
    ExpressionStatementResolver expressionStatementResolver = new ExpressionStatementResolver(systemContext);
    expressionStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(QueryAssignment statement, Namespace ns) {
    QueryStatementResolver queryStatementResolver = new QueryStatementResolver(systemContext);
    queryStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(JoinAssignment statement, Namespace ns) {
    JoinStatementResolver joinStatementResolver = new JoinStatementResolver(systemContext);
    joinStatementResolver.resolve(statement, ns);
    return null;
  }

  @Override
  public Void visit(DistinctAssignment statement, Namespace ns) {
    DistinctStatementResolver distinctStatementResolver = new DistinctStatementResolver(systemContext);
    distinctStatementResolver.resolve(statement, ns);
    return null;
  }
}
