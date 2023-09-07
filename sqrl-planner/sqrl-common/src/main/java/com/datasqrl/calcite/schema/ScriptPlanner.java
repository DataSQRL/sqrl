package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.CatalogReader;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlPreparingTable;
import com.datasqrl.calcite.SqrlRelBuilder;
import com.datasqrl.calcite.schema.SqrlToSql.Result;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.plan.rel.LogicalStream;
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
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

@AllArgsConstructor
public class ScriptPlanner implements SqrlStatementVisitor<LogicalOp, Void> {

  private QueryPlanner planner;

  RelMetadataProvider relMetadataProvider;

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
        .scanSqrl(tableName);

    RexNode node = builder.evaluateExpression(query.getTimestamp());
    if (!(node instanceof RexInputRef) && query.getTimestampAlias().isEmpty()) {
      builder.addColumnOp(node, "_time") //assign a hidden timestamp field for expressions
          .assignTimestampOp(-1);
    } else if (query.getTimestampAlias().isPresent()) {
      //otherwise, add new column
      builder.addColumnOp(node, query.getTimestampAlias().get().getSimple())
          .assignTimestampOp(-1);
    } else {
      builder.assignTimestampOp(((RexInputRef) node).getIndex());
    }

    return (LogicalOp)builder.build();
  }

  @Override
  public LogicalOp visit(SqrlJoinQuery query, Void context) {
    //we can resolve type by looking at the lhs relopttable
    SqrlPreparingTable table = planner.getCatalogReader()
        .getSqrlTable(SqrlListUtil.popLast(query.getIdentifier().names));

    TableResult result = planTable(query, query.getQuery(), false,
        query.getTableArgs());
    SqlValidator validator = planner.createSqlValidator();

    SqrlPreparingTable toTable = planner.getCatalogReader()
        .getSqrlTable(result.getResult().getCurrentPath());

    SqlNode node = planner.relToSql(Dialect.CALCITE, result.getRelNode());
    TableFunction function = createFunction(validator, result.def, toTable.getRowType(),
        node, toTable.getQualifiedName().get(0), planner.getCatalogReader());

    return new LogicalCreateReference(this.planner.getCluster(), null,
        query.getIdentifier().names, result.getResult().getCurrentPath(),
        Util.last(query.getIdentifier().names), result.getDef(), result.getRelNode(),
        function
    );
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

    RelNode exportRel = planner.getSqrlRelBuilder()
        .scanSqrl(node.getIdentifier().names)
        .projectAll(false)
        .buildAndUnshadow();

    List<String> path = node.getSinkPath().names;
    return new LogicalExportOp(planner.getCluster(), null, exportRel, table, path);
  }

  @Override
  public LogicalOp visit(SqrlDistinctQuery node, Void context) {
    String sqrlTableName = ((SqlIdentifier) node.getTable()).names.get(0);

    SqrlRelBuilder builder = planner.getSqrlRelBuilder();
    RelNode relNode = builder
        .scanSqrl(sqrlTableName)
        .projectAllPrefixDistinct(builder.evaluateExpressions(node.getOperands(), sqrlTableName))
        .sortLimit(0, 1, builder.evaluateOrderExpression(node.getOrder(), sqrlTableName))
        .projectAll()
        .distinctOnHint(node.getOperands().size())
        .buildAndUnshadow();

    relNode = RelOptUtil.propagateRelHints(relNode, false);

    System.out.println(planner.relToString(Dialect.CALCITE, relNode));
    System.out.println(relNode.explain());
    System.out.println();
    return new LogicalCreateTableOp(planner.getCluster(), null,
        relNode,
        createHints(node.getHints()), node.getHints(), node.getIdentifier().names, false, null,
        new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of()));
  }

  @Override
  public LogicalOp visit(SqrlStreamQuery node, Void context) {
    LogicalCreateTableOp relNode = createTable(node, node.getQuery());

    RelNode logicalStream = planner.getSqrlRelBuilder()
        .push(relNode.getInput(0))
        .stream(node.getType())
        .build();

    return new LogicalCreateStreamOp(planner.getCluster(), null,(LogicalStream) logicalStream,
        node.getIdentifier().names, relNode.fromRelOptTable, node.getHints(),
        node.getTableArgs().orElse(new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of())));
  }

  @Override
  public LogicalOp visit(SqrlSqlQuery node, Void context) {
    return createTable(node, node.getQuery());
  }

  private LogicalCreateTableOp createTable(SqrlAssignment node, SqlNode query) {
    TableResult result = planTable(node, query, true, node.getTableArgs());

    RelOptTable fromRelOptTable = null;
    List<SqrlTableParamDef> params = new ArrayList<>();
    if (node.getIdentifier().names.size() > 1) {
      fromRelOptTable = planner.getCatalogReader().getSqrlTable(SqrlListUtil.popLast(node.getIdentifier().names));

      for (int i = 0; i < fromRelOptTable.getKeys().get(0).asSet().size(); i++) {
        //create equality constraint of primary keys
        RelDataTypeField lhs = fromRelOptTable.getRowType().getFieldList().get(i);
        params.add(new SqrlTableParamDef(SqlParserPos.ZERO,
            new SqlIdentifier("@"+lhs.getName(), SqlParserPos.ZERO),
            SqlDataTypeSpecFactory.create(lhs.getType()),
                Optional.of(new SqlDynamicParam(lhs.getIndex(),SqlParserPos.ZERO)),
            params.size(), true));
      }
    }

    params.addAll(node.getTableArgs()
        .orElse(new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of()))
        .getParameters());

    return new LogicalCreateTableOp(planner.getCluster(), null,
        result.getRelNode(),
        createHints(node.getHints()),
        node.getHints(), node.getIdentifier().names,
        true, fromRelOptTable,new SqrlTableFunctionDef(SqlParserPos.ZERO, params) );
  }

  private TableResult planTable(SqrlAssignment node, SqlNode query, boolean materialSelfTable,
      Optional<SqrlTableFunctionDef> argDef) {
    List<String> currentPath = SqrlListUtil.popLast(node.getIdentifier().names);

    SqrlToSql sqrlToSql = new SqrlToSql(this.planner);
    Result result = sqrlToSql.rewrite(query, materialSelfTable, currentPath, argDef);

    Optional<SqrlPreparingTable> parentTable = node.getIdentifier().names.size() == 1
        ? Optional.empty()
        : Optional.of(planner.getCatalogReader().getSqrlTable(SqrlListUtil.popLast(node.getIdentifier().names)));

    TransformArguments transform =
        new TransformArguments(parentTable, node.getTableArgs(), materialSelfTable, planner.getCatalogReader().nameMatcher());
    SqlNode sqlNode = result.getSqlNode().accept(transform);
    SqrlTableFunctionDef newArguments = transform.getArgumentDef();

    System.out.println("Transformed: " + planner.sqlToString(Dialect.CALCITE, sqlNode));
    RelNode relNode = planner.plan(Dialect.CALCITE, sqlNode);
    SqrlRelBuilder builder = planner.getSqrlRelBuilder();
    relNode = builder.push(relNode)
        .buildAndUnshadow();//todo get original names
    System.out.println("Planned: " + planner.relToString(Dialect.CALCITE, relNode));
    System.out.println();
    return TableResult.of(result, relNode, sqlNode, newArguments);
  }

  @Override
  public LogicalOp visit(SqrlExpressionQuery node, Void context) {
    SqrlRelBuilder builder = planner.getSqrlRelBuilder()
        .scanSqrl(SqrlListUtil.popLast(node.getIdentifier().names));

    builder.addColumnOp(builder.evaluateExpression(node.getExpression()), Util.last(node.getIdentifier().names));
    return (LogicalOp) builder.build();
  }

  public static TableFunction createFunction(SqlValidator validator, SqrlTableFunctionDef def, RelDataType type,
      SqlNode node, String tableName, CatalogReader catalogReader) {
    SqrlTableFunction tableFunction = new SqrlTableFunction(toParams(def.getParameters(), validator),
        node, tableName, catalogReader);
    return tableFunction;
  }

  private static List<FunctionParameter> toParams(List<SqrlTableParamDef> params,
      SqlValidator validator) {
    List<FunctionParameter> parameters = params.stream()
        .map(p->new SqrlFunctionParameter(p.getName().getSimple(), p.getDefaultValue(),
            p.getType(), p.getIndex(), p.getType().deriveType(validator),p.isInternal()))
        .collect(Collectors.toList());
    return parameters;

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
