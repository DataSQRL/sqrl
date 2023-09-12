package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.CatalogReader;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.ModifiableSqrlTable;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlNameMatcher;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.TemporaryViewTable;
import com.datasqrl.calcite.TimestampAssignableTable;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.schema.sql.SqrlToSql;
import com.datasqrl.calcite.schema.sql.SqrlToSql.Result;
import com.datasqrl.calcite.validator.ScriptValidator;
import com.datasqrl.calcite.validator.ScriptValidator.QualifiedExport;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.rel.LogicalStream;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.SqlNameUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlAssignTimestamp;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlDistinctQuery;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.calcite.sql.SqrlFromQuery;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlJoinQuery;
import org.apache.calcite.sql.SqrlSqlQuery;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.SqrlStreamQuery;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.SqrlTableParamDef;
import org.apache.calcite.sql.StatementVisitor;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.ListUtils;

@AllArgsConstructor
public class ScriptPlanner implements StatementVisitor<Void, Void> {

  private QueryPlanner planner;
  private ScriptValidator validator;
  SqrlTableFactory tableFactory;
  SqrlFramework framework;
  SqlNameUtil nameUtil;
  ModuleLoader moduleLoader;
  ErrorCollector errors;

  public Void plan(SqlNode query) {
    if (query instanceof ScriptNode) {
      ScriptNode script = (ScriptNode) query;
      script.getStatements().stream()
          .map(this::plan)
          .collect(Collectors.toList());
      return null;
    } else if (query instanceof SqrlStatement) {
      return SqlNodeVisitor.accept(this, query, null);
    }

    throw new RuntimeException("Unrecognized operator:" + query);
  }

  @Override
  public Void visit(SqrlAssignTimestamp query, Void context) {
    List<String> tableName = query.getAlias().orElse(query.getIdentifier()).names;
    RelOptTable table = planner.getCatalogReader().getSqrlTable(tableName);

    RexNode node = planner.planExpression(query.getTimestamp(), table.getRowType());
    TimestampAssignableTable timestampAssignableTable = table.unwrap(TimestampAssignableTable.class);

    if (!(node instanceof RexInputRef) && query.getTimestampAlias().isEmpty()) {
      addColumn(node, "_time", table);
      timestampAssignableTable.assignTimestamp(-1);
    } else if (query.getTimestampAlias().isPresent()) {
      //otherwise, add new column
      addColumn(node, query.getTimestampAlias().get().getSimple(),
          planner.getCatalogReader().getSqrlTable(tableName));
      timestampAssignableTable.assignTimestamp(-1);
    } else {
      timestampAssignableTable.assignTimestamp(((RexInputRef) node).getIndex());
    }

    return null;
  }

  @Override
  public Void visit(SqrlJoinQuery query, Void context) {
    TableResult result = planTable(query, query.getQuery(), false, true,
        query.getTableArgs());

    RelOptTable toTable = planner.getCatalogReader()
        .getSqrlTable(result.getResult().getCurrentPath());

    RelBuilder b = planner.getRelBuilder()
        .push(result.getRelNode());
    projectRange(b, List.of(), toTable.getRowType().getFieldNames());

    RelNode relNode = planner.expandMacros(b.build());

    SqlNode node = planner.relToSql(Dialect.CALCITE, relNode);

    TableFunction function = createFunction(planner.createSqlValidator(),
        result.getDef(), toTable.getRowType(),
        node, toTable.getQualifiedName().get(0), planner.getCatalogReader());

    String name = framework.getSchema().getUniqueFunctionName(query.getIdentifier().names);
    framework.getSchema().plus().add(name, function);

    if (query.getIdentifier().names.size() > 1) {
      SQRLTable table = framework.getQueryPlanner().getCatalogReader()
          .getSqrlTable(SqrlListUtil.popLast(query.getIdentifier().names))
          .unwrap(ModifiableSqrlTable.class).getSqrlTable();
      SQRLTable toSqrlTable = toTable.unwrap(ModifiableSqrlTable.class).getSqrlTable();

      table.addRelationship(nameUtil.toName(Util.last(query.getIdentifier().names)), toSqrlTable, Relationship.JoinType.CHILD, Multiplicity.MANY);
      this.framework.getSchema().addRelationship(
          query.getIdentifier().names, toSqrlTable.getPath().toStringList());
    }

    return null;
  }

  public void projectRange(RelBuilder relBuilder, List<String> start, List<String> end) {
    List<RexNode> first = IntStream.range(0, start.size())
        .mapToObj(i->relBuilder.field(i))
        .collect(Collectors.toList());

    List<RexNode> proj = relBuilder.fields(
        ImmutableBitSet.range(relBuilder.peek().getRowType().getFieldCount() - end.size(),
            relBuilder.peek().getRowType().getFieldCount()));

    relBuilder.project(ListUtils.union(first, proj), ListUtils.union(start, end), true);
  }

  @Override
  public Void visit(SqrlFromQuery query, Void context) {
    throw new RuntimeException();
  }

  @Override
  public Void visit(SqrlImportDefinition node, Void context) {
    validator.getImportOps().get(node)
        .forEach(i->i.getObject().apply(i.getAlias(), framework, errors));
    return null;
  }

  @Override
  public Void visit(SqrlExportDefinition node, Void context) {
    QualifiedExport export = validator.getExportOps().get(node);
    RelOptTable table = planner.getCatalogReader().getSqrlTable(export.getTable());

    RelBuilder relBuilder = planner.getRelBuilder();
    RelNode relNode = relBuilder
        .scan(table.getQualifiedName())
        .project(relBuilder.fields()) //todo remove hidden fields
        .build();

    ResolvedExport resolvedExport = new ResolvedExport(table.getQualifiedName().get(0),
        relNode, export.getSink());

    framework.getSchema().add(resolvedExport);

    return null;
  }

  @Override
  public Void visit(SqrlDistinctQuery node, Void context) {
    String sqrlTableName = ((SqlIdentifier) node.getTable()).names.get(0);

    RelOptTable table = planner.getCatalogReader().getSqrlTable(((SqlIdentifier) node.getTable()).names);
    RelBuilder builder = planner.getRelBuilder();

    RelHint hint = RelHint.builder("DISTINCT_ON")
        .hintOptions(IntStream.range(0, node.getOperands().size())
            .mapToObj(Integer::toString)
            .collect(Collectors.toList()))
        .build();

    builder.scan(table.getQualifiedName());

    projectAllPrefixDistinct(builder, planner.planExpressions(node.getOperands(),
        new TemporaryViewTable(builder.peek().getRowType()),
        sqrlTableName));
    RelNode relNode = builder
        .sortLimit(0, 1, evaluateOrderExpression(builder, node.getOrder(), sqrlTableName))
        .project(builder.fields())
        .hints(hint)
        .build();

    relNode = planner.expandMacros(relNode);

    relNode = RelOptUtil.propagateRelHints(relNode, false);

    createTable(node.getIdentifier().names, relNode,createHints(node.getHints()),
        false,  node.getHints(), new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of()));

    return null;
  }

  public List<RexNode> evaluateOrderExpression(RelBuilder builder, List<SqlNode> order,
      String tableName) {
    List<RexNode> orderExprs = new ArrayList<>();
    //expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]
    for (SqlNode expr : order) {
      RexNode rex;
      switch (expr.getKind()) {
        case DESCENDING:
        case NULLS_FIRST:
        case NULLS_LAST:
          SqlCall op = (SqlCall) expr;
          rex = planner.planExpression(op.getOperandList().get(0), new TemporaryViewTable(builder.peek().getRowType()),
              tableName);
          rex = builder.call(op.getOperator(), rex);
          break;
        default:
          rex = planner.planExpression(expr, new TemporaryViewTable(builder.peek().getRowType()), tableName);
      }

      orderExprs.add(rex);
    }
    return orderExprs;
  }

  // SELECT key1, key2, * EXCEPT [key1 key2]
  public void projectAllPrefixDistinct(RelBuilder builder, Project firstNodes) {
    List<String> stripped = getLatestColumns(builder.peek().getRowType());

    for (RexNode n : firstNodes.getProjects()) {
      if (n instanceof RexInputRef) {
        String name = builder.peek().getRowType().getFieldNames()
            .get(((RexInputRef) n).getIndex());
        stripped.remove(name);
      }
    }

    List<String> names = new ArrayList<>();
    List<RexNode> project = new ArrayList<>();
    project.addAll(firstNodes.getProjects());
    names.addAll(firstNodes.getRowType().getFieldNames());
    for (String field : stripped) {
      RexNode f = builder.field(field);
      project.add(f);
      names.add(field);
    }

    builder.project(project, names, true);
  }

  private List<String> getLatestColumns(RelDataType fieldList) {
    return fieldList.getFieldList().stream()
        .map(RelDataTypeField::getName)
        .filter(fieldName -> {
          String latest = SqrlNameMatcher.getLatestVersion(fieldList.getFieldNames(),
              fieldName.split("\\$")[0]);
          return latest == null || latest.equals(fieldName);
        })
        .collect(Collectors.toList());
  }

  @Override
  public Void visit(SqrlStreamQuery node, Void context) {
    TableResult result = planTable(node, node.getQuery());

    RelNode input = result.getRelNode();
    LogicalStream stream = LogicalStream.create(input, node.getType());

    createTable(node.getIdentifier().names, stream, createHints(node.getHints()),
        true,  node.getHints(), result.def);
    return null;
  }

  @Override
  public Void visit(SqrlSqlQuery node, Void context) {
    TableResult result = planTable(node, node.getQuery());
    createTable(node.getIdentifier().names, result.getRelNode(), createHints(node.getHints()),
        true, node.getHints(), result.def);

    return null;
  }

  private TableResult planTable(SqrlAssignment node, SqlNode query) {
    if (node.getTableArgs().isPresent()) {
      return planTable(node, query, false, false, node.getTableArgs());
    } else {

      TableResult result = planTable(node, query, true, false, node.getTableArgs());

      RelOptTable fromRelOptTable = null;
      List<SqrlTableParamDef> params = new ArrayList<>();
      if (node.getIdentifier().names.size() > 1) {
        fromRelOptTable = planner.getCatalogReader()
            .getSqrlTable(SqrlListUtil.popLast(node.getIdentifier().names));

        for (int i = 0; i < fromRelOptTable.getKeys().get(0).asSet().size(); i++) {
          //create equality constraint of primary keys
          RelDataTypeField lhs = fromRelOptTable.getRowType().getFieldList().get(i);
          params.add(new SqrlTableParamDef(SqlParserPos.ZERO,
              new SqlIdentifier("@" + lhs.getName(), SqlParserPos.ZERO),
              SqlDataTypeSpecBuilder.create(lhs.getType()),
              Optional.of(new SqlDynamicParam(lhs.getIndex(), SqlParserPos.ZERO)),
              params.size(), true));
        }
      }

      params.addAll(node.getTableArgs()
          .orElse(new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of()))
          .getParameters());

      return new TableResult(result.result, result.relNode, result.sqlNode, new SqrlTableFunctionDef(SqlParserPos.ZERO, params));
    }
  }

  private TableResult planTable(SqrlAssignment node, SqlNode query, boolean materialSelfTable,
      boolean isJoinDeclaration, Optional<SqrlTableFunctionDef> argDef) {
    List<String> currentPath = SqrlListUtil.popLast(node.getIdentifier().names);

    SqrlToSql sqrlToSql = new SqrlToSql(this.planner);
    Result result = sqrlToSql.rewrite(query, materialSelfTable, currentPath, argDef, isJoinDeclaration);

    Optional<RelOptTable> parentTable = node.getIdentifier().names.size() == 1
        ? Optional.empty()
        : Optional.of(planner.getCatalogReader().getSqrlTable(SqrlListUtil.popLast(node.getIdentifier().names)));

    TransformArguments transform =
        new TransformArguments(parentTable, node.getTableArgs(), materialSelfTable, planner.getCatalogReader().nameMatcher());
    SqlNode sqlNode = result.getSqlNode().accept(transform);
    SqrlTableFunctionDef newArguments = transform.getArgumentDef();

    RelNode relNode = planner.plan(Dialect.CALCITE, sqlNode);
    relNode = planner.getRelBuilder()
        .push(relNode)
        .build();
    relNode = planner.expandMacros(relNode);
    return TableResult.of(result, relNode, sqlNode, newArguments);
  }

  @Override
  public Void visit(SqrlExpressionQuery node, Void context) {
    RelOptTable table = planner.getCatalogReader().getSqrlTable(SqrlListUtil.popLast(node.getIdentifier().names));
    RexNode rexNode = planner.planExpression(node.getExpression(), table.getRowType());

    addColumn(rexNode, Util.last(node.getIdentifier().names), table);
    return null;
  }

  private void addColumn(RexNode node, String columnNmae, RelOptTable table) {
    if (table.unwrap(ModifiableSqrlTable.class) != null) {
      ModifiableSqrlTable table1 = (ModifiableSqrlTable) table.unwrap(Table.class);
      String name = uniquifyColumnName(columnNmae, table1.getRowType(null).getFieldNames());
      table1.addColumn(nameUtil.toName(name).getCanonical(), node, framework.getTypeFactory());
      SQRLTable sqrlTable = table1.getSqrlTable();
      sqrlTable.addColumn(Name.system(columnNmae), nameUtil.toName(name),true, node.getType());
    } else {
      throw new RuntimeException();
    }
  }

  public static String uniquifyColumnName(String name, List<String> names) {
    return name + "$" + SqrlNameMatcher.getNextVersion(names, name);
  }

  private void createTable(List<String> path, RelNode input, List<RelHint> hints,
      boolean setFieldNames, Optional<SqlNodeList> opHints, SqrlTableFunctionDef args) {
    //todo: uniform
    if (args.getParameters().isEmpty() || !hasPublicArgs(args.getParameters())) {
      tableFactory.createTable(path, input, hints, setFieldNames, opHints, args);
    } else {
      SqlNode node = framework.getQueryPlanner().relToSql(Dialect.CALCITE, input);

      TableFunction function = createFunction(
          this.framework.getQueryPlanner().createSqlValidator(),
          args,
          input.getRowType(), node, path.get(0) + "$" + 0,
          framework.getQueryPlanner().getCatalogReader());

      this.framework.getSchema().plus().add(path.get(0) + "$" + 0, function);
      SQRLTable sqrlTable = new SQRLTable(
          NamePath.of(path.toArray(String[]::new)), null, 0);
      for (RelDataTypeField name : input.getRowType().getFieldList()) {
        sqrlTable.addColumn(Name.system(name.getName().split("\\$")[0]), Name.system(name.getName()), true,
            name.getType());
      }

      framework.getSchema().addSqrlTable(sqrlTable);
    }
  }

  private boolean hasPublicArgs(List<SqrlTableParamDef> parameters) {
    return parameters.stream()
        .anyMatch(f->!f.isInternal());
  }

  public static TableFunction createFunction(SqlValidator validator, SqrlTableFunctionDef def, RelDataType type,
      SqlNode node, String tableName, CatalogReader catalogReader) {
    SqrlTableFunction tableFunction = new SqrlTableFunction(toParams(def.getParameters(), validator),
        node, tableName, catalogReader, Optional.ofNullable(type));
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


//
//  public RelBuilder project(List<String> fieldNames) {
//    //append field names to end
//    List<RelDataTypeField> fieldList = peek().getRowType().getFieldList();
//    int offset = fieldList.size() - fieldNames.size();
//
//    List<RexNode> rexNodes = new ArrayList<>();
//    List<String> names = new ArrayList<>();
//    for (int i = 0; i < fieldList.size(); i++) {
//      rexNodes.add(field(i));
//      if (offset < i) {
//        names.add(fieldList.get(i).getName());
//      } else {
//        names.add(fieldNames.get(i - offset));
//      }
//
//    }
//    project(rexNodes, names, true);
//    return this;
//  }

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
