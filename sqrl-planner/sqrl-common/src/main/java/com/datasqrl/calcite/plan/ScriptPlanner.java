package com.datasqrl.calcite.plan;

import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.SqlNameUtil;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlAssignTimestamp;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlDistinctQuery;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.calcite.sql.SqrlFromQuery;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlJoinQuery;
import org.apache.calcite.sql.SqrlSqlQuery;
import org.apache.calcite.sql.SqrlStreamQuery;
import org.apache.calcite.sql.StatementVisitor;

@AllArgsConstructor
public class ScriptPlanner implements StatementVisitor<Void, Void> {

  private final QueryPlanner planner;
  private final com.datasqrl.plan.validate.ScriptPlanner validator;
  private final SqrlTableFactory tableFactory;
  private final SqrlFramework framework;
  private final SqlNameUtil nameUtil;
  private final ErrorCollector errors;

  public Void plan(SqlNode query) {
    return SqlNodeVisitor.accept(this, query, null);
  }

  @Override
  public Void visit(SqrlStreamQuery statement, Void context) {
    return StatementVisitor.super.visit(statement, context);
  }

  @Override
  public Void visit(SqrlSqlQuery statement, Void context) {
    return StatementVisitor.super.visit(statement, context);
  }

  @Override
  public Void visit(SqrlJoinQuery statement, Void context) {
    return StatementVisitor.super.visit(statement, context);
  }

  @Override
  public Void visit(SqrlFromQuery statement, Void context) {
    return StatementVisitor.super.visit(statement, context);
  }

  @Override
  public Void visit(SqrlDistinctQuery statement, Void context) {
    return StatementVisitor.super.visit(statement, context);
  }

  @Override
  public Void visit(SqrlAssignment statement, Void context) {
//    return StatementVisitor.super.visit(statement, context);
    return null;
  }

  @Override
  public Void visit(SqrlImportDefinition statement, Void context) {
    return null;
  }

  @Override
  public Void visit(SqrlExportDefinition statement, Void context) {
    return null;
  }

  @Override
  public Void visit(SqrlAssignTimestamp statement, Void context) {
    return null;
  }

  @Override
  public Void visit(SqrlExpressionQuery node, Void context) {

    return null;
  }

}
