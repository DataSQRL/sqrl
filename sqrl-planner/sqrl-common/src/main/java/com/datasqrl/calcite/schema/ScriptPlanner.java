package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlPreparingTable;
import com.datasqrl.calcite.SqrlRelBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.schema.sql.SqrlToSql;
import com.datasqrl.calcite.schema.sql.SqrlToSql.Result;
import com.datasqrl.calcite.schema.op.LogicalCreateTableOp;
import com.datasqrl.calcite.schema.op.LogicalImportOp;
import com.datasqrl.calcite.schema.op.LogicalOp;
import com.datasqrl.calcite.schema.op.LogicalSchemaModifyOps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

@AllArgsConstructor
public class ScriptPlanner implements SqrlStatementVisitor<LogicalOp, Void> {

  private QueryPlanner planner;

  public RelNode plan(SqlNode query) {
    if (query instanceof ScriptNode) {
      ScriptNode script = (ScriptNode) query;
      List<RelNode> ops = script.getStatements().stream()
          .map(this::plan)
          .collect(Collectors.toList());
      return new LogicalSchemaModifyOps(planner.getCluster(), null, ops);
    } else if (query instanceof SqrlStatement) {
      return ((SqrlStatement) query).accept(this, null);
    }

    throw new RuntimeException("Unrecognized operator:" + query);
  }

  @Override
  public LogicalOp visit(SqrlAssignTimestamp query, Void context) {
    List<String> tableName = query.getAlias().orElse(query.getIdentifier()).names;

    SqrlRelBuilder builder = planner.getSqrlRelBuilder()
        .scan(planner.getCatalogReader().getSqrlTable(tableName).getInternalTable().getQualifiedName());

    RexNode node = builder.evaluateExpression(query.getTimestamp());
    if (!(node instanceof RexInputRef) && query.getTimestampAlias().isEmpty()) {
      builder.addColumnOp(node, "_time",
              planner.getCatalogReader().getSqrlTable(tableName)) //assign a hidden timestamp field for expressions
          .assignTimestampOp(-1);
    } else if (query.getTimestampAlias().isPresent()) {
      //otherwise, add new column
      builder.addColumnOp(node, query.getTimestampAlias().get().getSimple(),
              planner.getCatalogReader().getSqrlTable(tableName))
          .assignTimestampOp(-1);
    } else {
      builder.assignTimestampOp(((RexInputRef) node).getIndex());
    }

    return (LogicalOp)builder.build();
  }

  @Override
  public LogicalOp visit(SqrlJoinQuery query, Void context) {
    TableResult result = planTable(query, query.getQuery(), false, true,
        query.getTableArgs());

    SqrlPreparingTable toTable = planner.getCatalogReader()
        .getSqrlTable(result.getResult().getCurrentPath());

    return (LogicalOp) planner.getSqrlRelBuilder()
        .push(result.getRelNode())
        .projectLast(List.of(), toTable.getRowType().getFieldNames())
        .createReferenceOp(
            query.getIdentifier().names,
            List.of(result.getResult().getCurrentPath()),
            result.getDef())
        .build();
  }

  @Override
  public LogicalOp visit(SqrlFromQuery query, Void context) {
    throw new RuntimeException();
  }

  @Override
  public LogicalOp visit(SqrlImportDefinition node, Void context) {
    return new LogicalImportOp(planner.getCluster(), null, node.getImportPath().names,
        node.getAlias().map(SqlIdentifier::getSimple));
  }

  @Override
  public LogicalOp visit(SqrlExportDefinition node, Void context) {
    RelOptTable table = planner.getCatalogReader().getSqrlTable(node.getIdentifier().names);
    return (LogicalOp) planner.getSqrlRelBuilder()
        .scan(table.getQualifiedName())
        .projectAll() //todo remove hidden fields
        .export(node.getSinkPath().names)
        .build();
  }

  /**
   * I need to retain all of the original names
   */
  @Override
  public LogicalOp visit(SqrlDistinctQuery node, Void context) {
    String sqrlTableName = ((SqlIdentifier) node.getTable()).names.get(0);

    SqrlPreparingTable table = planner.getCatalogReader().getSqrlTable(((SqlIdentifier) node.getTable()).names);
    SqrlRelBuilder builder = planner.getSqrlRelBuilder();
    RelNode relNode = builder
        .scan(table.getInternalTable().getQualifiedName())
        .projectAllPrefixDistinct(builder.evaluateExpressionsShadowing(node.getOperands(), sqrlTableName))
        //todo: shadowing here too
        .sortLimit(0, 1, builder.evaluateOrderExpression(node.getOrder(), sqrlTableName))
        .projectAll() //wrong
        .distinctOnHint(node.getOperands().size())
        .buildAndUnshadow(); //

    System.out.println(relNode.explain());
    System.out.println(planner.relToString(Dialect.CALCITE, relNode));


    relNode = RelOptUtil.propagateRelHints(relNode, false);

    return new LogicalCreateTableOp(planner.getCluster(), null,
        relNode,
        createHints(node.getHints()), node.getHints(), node.getIdentifier().names, false, null,
        new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of()));
  }

  @Override
  public LogicalOp visit(SqrlStreamQuery node, Void context) {
    LogicalCreateTableOp relNode = createTable(node, node.getQuery());

    return (LogicalOp) planner.getSqrlRelBuilder()
        .push(relNode.getInput(0))
        .stream(node.getType())
        .createStreamOp(node.getIdentifier().names, node.getHints(),
            node.getTableArgs().orElse(new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of())),
            relNode.getFromRelOptTable())
        .build();
  }

  @Override
  public LogicalOp visit(SqrlSqlQuery node, Void context) {
    return createTable(node, node.getQuery());
  }

  private LogicalCreateTableOp createTable(SqrlAssignment node, SqlNode query) {
    TableResult result = planTable(node, query, true, false, node.getTableArgs());

    RelOptTable fromRelOptTable = null;
    List<SqrlTableParamDef> params = new ArrayList<>();
    if (node.getIdentifier().names.size() > 1) {
      fromRelOptTable = planner.getCatalogReader().getSqrlTable(SqrlListUtil.popLast(node.getIdentifier().names));

      for (int i = 0; i < fromRelOptTable.getKeys().get(0).asSet().size(); i++) {
        //create equality constraint of primary keys
        RelDataTypeField lhs = fromRelOptTable.getRowType().getFieldList().get(i);
        params.add(new SqrlTableParamDef(SqlParserPos.ZERO,
            new SqlIdentifier("@"+lhs.getName(), SqlParserPos.ZERO),
            SqlDataTypeSpecBuilder.create(lhs.getType()),
                Optional.of(new SqlDynamicParam(lhs.getIndex(),SqlParserPos.ZERO)),
            params.size(), true));
      }
    }

    params.addAll(node.getTableArgs()
        .orElse(new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of()))
        .getParameters());

    System.out.println(result.relNode.explain());
    System.out.println(planner.relToString(Dialect.CALCITE, result.relNode));
    return new LogicalCreateTableOp(planner.getCluster(), null,
        result.getRelNode(),
        createHints(node.getHints()),
        node.getHints(), node.getIdentifier().names,
        true, fromRelOptTable,new SqrlTableFunctionDef(SqlParserPos.ZERO, params) );
  }

  private TableResult planTable(SqrlAssignment node, SqlNode query, boolean materialSelfTable,
      boolean isJoinDeclaration, Optional<SqrlTableFunctionDef> argDef) {
    List<String> currentPath = SqrlListUtil.popLast(node.getIdentifier().names);

    SqrlToSql sqrlToSql = new SqrlToSql(this.planner);
    Result result = sqrlToSql.rewrite(query, materialSelfTable, currentPath, argDef, isJoinDeclaration);

    Optional<SqrlPreparingTable> parentTable = node.getIdentifier().names.size() == 1
        ? Optional.empty()
        : Optional.of(planner.getCatalogReader().getSqrlTable(SqrlListUtil.popLast(node.getIdentifier().names)));

    TransformArguments transform =
        new TransformArguments(parentTable, node.getTableArgs(), materialSelfTable, planner.getCatalogReader().nameMatcher());
    SqlNode sqlNode = result.getSqlNode().accept(transform);
    SqrlTableFunctionDef newArguments = transform.getArgumentDef();

    System.out.println("ToPlan: "+planner.sqlToString(Dialect.CALCITE, sqlNode));
    RelNode relNode = planner.plan(Dialect.CALCITE, sqlNode);
    System.out.println("Planned: "+planner.relToString(Dialect.CALCITE, relNode));
    relNode = planner.getSqrlRelBuilder()
        .push(relNode)
        .buildAndUnshadow();//todo get original names
    System.out.println("Planned2: "+planner.relToString(Dialect.CALCITE, relNode));
    return TableResult.of(result, relNode, sqlNode, newArguments);
  }

  @Override
  public LogicalOp visit(SqrlExpressionQuery node, Void context) {
    SqrlRelBuilder builder = planner.getSqrlRelBuilder();
    return (LogicalOp) builder
        .scanSqrl(SqrlListUtil.popLast(node.getIdentifier().names))
        .addColumnOp(builder.evaluateExpression(node.getExpression()),
            Util.last(node.getIdentifier().names),
            planner.getCatalogReader().getSqrlTable(SqrlListUtil.popLast(node.getIdentifier().names)))
        .build();
  }

  private List<RelHint> createHints(Optional<SqlNodeList> hints) {
    return hints.map(nodes -> nodes.getList().stream()
            .map(node -> RelHint.builder(((SqlHint) node).getName())
                .build())
            .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
  }

  @Value
  public static class TableResult {
    Result result;
    RelNode relNode;
    SqlNode sqlNode;
    SqrlTableFunctionDef def;

    public static TableResult of(Result result, RelNode relNode, SqlNode sqlNode,
        SqrlTableFunctionDef def) {
      return new TableResult(result, relNode, sqlNode, def);
    }
  }
}
