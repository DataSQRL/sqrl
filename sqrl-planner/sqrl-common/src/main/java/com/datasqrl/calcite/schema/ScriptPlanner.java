package com.datasqrl.calcite.schema;

import static com.datasqrl.plan.ScriptValidator.isSelfField;
import static com.datasqrl.plan.ScriptValidator.isSelfTable;
import static com.datasqrl.plan.ScriptValidator.isVariable;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.ModifiableSqrlTable;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.TimestampAssignableTable;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlAliasCallBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlJoinBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.schema.sql.SqlJoinPathBuilder;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.calcite.visitor.SqlRelationVisitor;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.ScriptValidator;
import com.datasqrl.plan.ScriptValidator.QualifiedExport;
import com.datasqrl.plan.hints.TopNHint.Type;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.rel.LogicalStream;
import com.datasqrl.schema.Column;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.tuple.Pair;

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
    return SqlNodeVisitor.accept(this, query, null);
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
  public Void visit(SqrlAssignment assignment, Void context) {
    SqlNode node = validator.getPreprocessSql().get(assignment);

    boolean materializeSelf = validator.getIsMaterializeTable().get(assignment);

    List<String> parentPath = SqrlListUtil.popLast(assignment.getIdentifier().names);
    Result result = new SqrlToSql().rewrite(node, materializeSelf, parentPath);

    //Expanding table functions may have added additional parameters that we need to remove.
    //These can only be discovered either during sqrl to sql rewriting or directly after
    List<FunctionParameter> parameters = validator.getParameters().get(assignment);
    Pair<List<FunctionParameter>, SqlNode> rewritten = extractSelfArgs(parameters,
        materializeSelf, result.getSqlNode());
    parameters = rewritten.getLeft();
    System.out.println(rewritten.getRight());

    RelNode relNode = planner.plan(Dialect.CALCITE, rewritten.getRight());
    System.out.println(relNode.explain());
    RelNode expanded = planner.expandMacros(relNode);
    System.out.println(expanded.explain());
    final Optional<SqlNode> sql;
    if (!materializeSelf) {
      sql = Optional.of(planner.relToSql(Dialect.CALCITE, expanded));
    } else {
      sql = Optional.empty();
    }

    if (assignment instanceof SqrlStreamQuery) {
      expanded = LogicalStream.create(expanded, ((SqrlStreamQuery)assignment).getType());
    }

    boolean setFieldNames = validator.getSetFieldNames().get(assignment);
    List<Function> isA = validator.getIsA().get(node);

    //Short path: if we're not materializing, create relationship
    if (!materializeSelf) {
      NamePath path = nameUtil.toNamePath(assignment.getIdentifier().names);
      List<SQRLTable> isASqrl = isA.stream()
          .map(f->((SqrlTableMacro)f).getSqrlTable())
          .collect(Collectors.toList());
      Supplier<RelNode> nodeSupplier = ()->framework.getQueryPlanner().plan(Dialect.CALCITE, sql.get());
      //if nested, add as relationship
      if (assignment.getIdentifier().names.size() > 1) {
        SQRLTable parent = ((SqrlTableMacro)planner.getTableFunction(path.popLast().toStringList()).getFunction())
            .getSqrlTable();

        Relationship rel = new Relationship(path.getLast(),
            path, framework.getUniqueColumnInt().incrementAndGet(),
            parent, Relationship.JoinType.CHILD, Multiplicity.MANY,
            isASqrl, parameters, nodeSupplier
            );
        parent.getSqrlTable().addRelationship(rel);
        planner.getSchema().addRelationship(rel);

        //Also add parent
        Relationship relationship = tableFactory.createParent(path, parent, isASqrl.get(0));
        isASqrl.get(0).addRelationship(relationship);
        planner.getSchema().addRelationship(relationship);
      } else {
        RootSqrlTable sqrlTable = new RootSqrlTable(path.getFirst(),
            null, isASqrl, parameters, nodeSupplier);

        List<String> fieldNames = relNode.getRowType().getFieldNames().stream()
            .map(f->f.contains("$") ? f.split("\\$")[0] : f)
            .collect(Collectors.toList());
        for (int i = 0; i < fieldNames.size(); i++) {
          String name = fieldNames.get(i);
          RelDataTypeField field = relNode.getRowType().getFieldList().get(i);
          sqrlTable.addColumn(framework, nameUtil.toName(name), nameUtil.toName(field.getName()), true,
              field.getType());
        }

        planner.getSchema().addSqrlTable(sqrlTable);
//        tableFactory.createTable(assignment.getIdentifier().names, expanded, null, setFieldNames,
//            assignment.getHints(), parameters, isA,
//            materializeSelf, Optional.of(nodeSupplier));
      }
    } else {
      tableFactory.createTable(assignment.getIdentifier().names, expanded, null, setFieldNames,
          assignment.getHints(), parameters, isA,
          materializeSelf, Optional.empty());
    }

    return null;
  }

  private Pair<List<FunctionParameter>, SqlNode> extractSelfArgs(List<FunctionParameter> parameters,
      boolean materializeSelf, SqlNode sqlNode) {
    if (materializeSelf) {
      return Pair.of(parameters, sqlNode);
    }
    List<FunctionParameter> newParams = new ArrayList<>(parameters);

    SqlNode node = sqlNode.accept(new SqlShuttle(){
      @Override
      public SqlNode visit(SqlCall call) {
        if (call.getKind() == SqlKind.OTHER_FUNCTION && call.getOperator() instanceof SqlUserDefinedTableFunction) {
          return call.getOperator().createCall(call.getParserPosition(),
              call.getOperandList().stream()
                  .map(o->o.accept(new SqlShuttle(){
                    @Override
                    public SqlNode visit(SqlIdentifier id) {
                      //if self, check if param list, if not create one
                      if (!isSelfField(id.names)){
                        return id;
                      }

                      for (FunctionParameter p : newParams) {
                        if (validator.getParamMapping().containsKey(p)) {
                          return validator.getParamMapping().get(p);
                        }
                      }

                      RelDataType anyType = planner.getTypeFactory().createSqlType(SqlTypeName.ANY);
                      SqrlFunctionParameter functionParameter = new SqrlFunctionParameter(id.names.get(1),
                          Optional.empty(), SqlDataTypeSpecBuilder
                          .create(anyType), newParams.size(), anyType,
                          true);
                      newParams.add(functionParameter);
                      SqlDynamicParam param = new SqlDynamicParam(functionParameter.getOrdinal(), id.getParserPosition());
                      validator.getParamMapping().put(functionParameter, param);

                      return param;
                    }
                  }))
                  .collect(Collectors.toList())
              );

        }

        return super.visit(call);
      }
    });

    return Pair.of(newParams, node);
  }

  @Override
  public Void visit(SqrlExpressionQuery node, Void context) {
    RelOptTable table = planner.getCatalogReader().getSqrlTable(SqrlListUtil.popLast(node.getIdentifier().names));
    RexNode rexNode = planner.planExpression(node.getExpression(), table.getRowType());

    addColumn(rexNode, Util.last(node.getIdentifier().names), table);
    return null;
  }

  private void addColumn(RexNode node, String cName, RelOptTable table) {
    if (table.unwrap(ModifiableSqrlTable.class) != null) {
      ModifiableSqrlTable table1 = (ModifiableSqrlTable) table.unwrap(Table.class);
      String name = uniquifyColumnName(cName);
      table1.addColumn(nameUtil.toName(name).getCanonical(), node, framework.getTypeFactory());
      SQRLTable sqrlTable = table1.getSqrlTable();
      sqrlTable.addColumn(framework, Name.system(cName), nameUtil.toName(name),true, node.getType());
    } else {
      throw new RuntimeException();
    }
  }

  public String uniquifyColumnName(String name) {
    if (name.contains("$")) {
      return name; //keep name as-is
    }

    return name + "$" + framework.getUniqueColumnInt().incrementAndGet();
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

  public class SqrlToSql implements SqlRelationVisitor<Result, Context> {
    final AtomicInteger pkId = new AtomicInteger(0);

    public Result rewrite(SqlNode query, boolean materializeSelf, List<String> currentPath) {
      Context context = new Context(materializeSelf, currentPath, new HashMap<>(), false, currentPath.size() > 0, false);

      Result result = SqlNodeVisitor.accept(this, query, context);
      CalciteFixes.appendSelectLists(result.getSqlNode());
      return result;
    }

    @Override
    public Result visitQuerySpecification(SqlSelect call, Context context) {
      boolean isAggregating = hasAggs(call.getSelectList().getList());
      // Copy query specification with new RelNode.
      Context newContext = new Context(context.materializeSelf, context.currentPath, new HashMap<>(), isAggregating,
          context.isNested,call.getFetch() != null);
      Result result = SqlNodeVisitor.accept(this, call.getFrom(), newContext);

      //retain distinct hint too
      /**
//       SELECT
//       /*+ `DISTINCT_ON`(`0`) */
//*
//      FROM (SELECT `customerid`, `Customer`.`_uuid` AS `_uuid`, `Customer`.`_ingest_time` AS `_ingest_time`, `Customer`.`email` AS `email`, `Customer`.`name` AS `name`, `Customer`.`lastupdated` AS `lastUpdated`
//      FROM `customer$10` AS `Customer`
//      ORDER BY `Customer`.`_ingest_time` DESC
//      FETCH NEXT 1 ROWS ONLY)
//       */
      if (isDistinctOnHintPresent(call)) {
        //1. get table, distinct on
        List<Integer> hintOps = IntStream.range(0, call.getSelectList().size())
            .boxed()
            .collect(Collectors.toList());

        //create new sql node list
        SqlSelectBuilder sqlSelectBuilder = new SqlSelectBuilder(call)
            .setLimit(1)
            .clearKeywords()
            .setFrom(result.sqlNode);

        Set<String> fieldNames = new HashSet<>(getFieldNames(call.getSelectList().getList()));
        List<SqlNode> selectList = new ArrayList<>(call.getSelectList().getList());
        //get latest fields not in select list

        List<Column> columns = planner.getCatalogReader().getSqrlTable(result.getCurrentPath())
            .unwrap(ModifiableSqrlTable.class)
            .getSqrlTable().getFields().getColumns(true);

        for (Column column : columns) {
          if (!fieldNames.contains(column.getName().getCanonical())) {
            selectList.add(new SqlIdentifier(column.getVtName().getCanonical(), SqlParserPos.ZERO));
          }
        }

        sqlSelectBuilder.setSelectList(selectList);
        //todo: wrap in select again?
        SqlSelect top = new SqlSelectBuilder()
            .setFrom(sqlSelectBuilder.build())
            .setDistinctOnHint(hintOps)
            .build();

        return new Result(top,
            result.getCurrentPath(), List.of(), List.of(), Optional.empty());
      } else if (call.isKeywordPresent(SqlSelectKeyword.DISTINCT) ||
          (context.isNested() && call.getFetch() != null)) {
        //if is nested, get primary key nodes
        int keySize = context.isNested()
            ? planner.getCatalogReader().getSqrlTable(context.currentPath).getKeys().get(0).asSet().size()
            : 0;
//      Preconditions.checkState(keySize == result.keysToPullUp.size());

        SqlSelectBuilder inner = new SqlSelectBuilder(call)
            .clearKeywords()
            .setFrom(result.getSqlNode())
            .rewriteExpressions(new WalkExpressions(planner, newContext));
        pullUpKeys(inner, result.keysToPullUp, isAggregating);

        SqlSelectBuilder topSelect = new SqlSelectBuilder()
            .setFrom(inner.build())
            .setTopNHint(call.isKeywordPresent(SqlSelectKeyword.DISTINCT)
                ? Type.SELECT_DISTINCT : Type.TOP_N, SqlSelectBuilder.sqlIntRange(keySize))
            ;

        return new Result(topSelect.build(),
            result.getCurrentPath(), List.of(), List.of(), Optional.empty());
      }

      SqlSelectBuilder select = new SqlSelectBuilder(call)
          .setFrom(result.getSqlNode())
          .rewriteExpressions(new WalkExpressions(planner, newContext));
      pullUpKeys(select, result.keysToPullUp, isAggregating);

      return new Result(select.build(), result.getCurrentPath(), List.of(), List.of(), Optional.empty());
    }

    private List<String> getFieldNames(List<SqlNode> list) {
      List<String> nodes = new ArrayList<>();
      for (SqlNode node : list) {
        if (node instanceof SqlIdentifier) {
          nodes.add(((SqlIdentifier) node).names.get(((SqlIdentifier) node).names.size()-1));
        } else if (node instanceof SqlCall && ((SqlCall)node).getKind() == SqlKind.AS) {
          nodes.add(((SqlIdentifier)((SqlCall) node).getOperandList().get(1)).names.get(0));
        } else {
          throw new RuntimeException("Could not derive name: " + node);
        }
      }

      return nodes;
    }

    private boolean isDistinctOnHintPresent(SqlSelect call) {
      return call.getHints().getList().stream()
          .anyMatch(f->((SqlHint)f).getName().equalsIgnoreCase("DISTINCT_ON"));
    }

    private void pullUpKeys(SqlSelectBuilder inner, List<String> keysToPullUp, boolean isAggregating) {
      if (!keysToPullUp.isEmpty()) {
        inner.prependSelect(keysToPullUp);
        if (isAggregating) {
          if (inner.hasOrder()) {
            inner.prependOrder(keysToPullUp);
          }
          inner.prependGroup(keysToPullUp);
        }
      }
    }

    private boolean hasAggs(List<SqlNode> list) {
      AtomicBoolean b = new AtomicBoolean(false);
      for (SqlNode node : list) {
        node.accept(new SqlShuttle() {
          @Override
          public SqlNode visit(SqlCall call) {
            if (call.getOperator() instanceof SqlUnresolvedFunction) {
              List<SqlOperator> matches = new ArrayList<>();
              planner.getOperatorTable().lookupOperatorOverloads(call.getOperator().getNameAsId(),
                  SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, matches, planner.getCatalogReader()
                      .nameMatcher());

              for (SqlOperator op : matches) {
                if (op.isAggregator()) {
                  b.set(true);
                }
              }
            } else {
              if (call.getOperator().isAggregator()) {
                b.set(true);
              }
            }
            return super.visit(call);
          }
        });
      }

      return b.get();
    }

    @Override
    public Result visitAliasedRelation(SqlCall node, Context context) {
      Result result = SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);

      SqlAliasCallBuilder aliasBuilder = new SqlAliasCallBuilder(node);
      context.addAlias(aliasBuilder.getAlias(), result.getCurrentPath());

      SqlNode newNode = aliasBuilder.setTable(result.getSqlNode())
          .build();

      return new Result(newNode, result.getCurrentPath(), result.keysToPullUp, List.of(), Optional.empty());
    }

    @Override
    public Result visitTable(SqrlCompoundIdentifier node, Context context) {
      Iterator<SqlNode> input = node.getItems().iterator();
      PathWalker pathWalker = new PathWalker(planner);

      SqlNode item = input.next();

      String identifier = getIdentifier(item)
          .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));

      SqlJoinPathBuilder builder = new SqlJoinPathBuilder(planner);
      boolean isSingleTable = node.getItems().size() == 1;
      boolean isAlias = context.hasAlias(identifier);
      boolean isAggregating = context.isAggregating();
      boolean isLimit = context.isLimit();
      boolean isNested = context.isNested();
      boolean isSelf = identifier.equals(ReservedName.SELF_IDENTIFIER.getCanonical());
      boolean materializeSelf = context.isMaterializeSelf();
//      boolean isSchemaTable = getIdentifier(item)
//          .map(i->planner.getCatalogReader().getSqrlTable(List.of(i)) != null)
//          .orElse(false);
      SqlUserDefinedTableFunction tableFunction = planner.getTableFunction(List.of(identifier));

      List<String> pullupColumns = List.of();
      if (item.getKind() == SqlKind.SELECT) {
        SqrlToSql sqrlToSql = new SqrlToSql();
        Result rewrite = sqrlToSql.rewrite(item, false, context.currentPath);
        RelNode relNode = planner.plan(Dialect.CALCITE, rewrite.getSqlNode());
        builder.push(rewrite.getSqlNode(), relNode.getRowType());
      } else if (tableFunction != null) {
        pathWalker.walk(identifier);
        builder.scanFunction(pathWalker.getPath(), List.of());
      } else if (isAlias) {
        if (!input.hasNext()) {
          throw new RuntimeException("Alias by itself.");
        }

        pathWalker.setPath(context.getAliasPath(identifier));
        //Walk the next one and push in table function
        String alias = identifier;
        item = input.next();
        String nextIdentifier = getIdentifier(item)
            .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));

        pathWalker.walk(nextIdentifier);

        SqlUserDefinedTableFunction fnc = planner.getTableFunction(pathWalker.getPath());
        if (fnc == null) {
          builder.scanNestedTable(pathWalker.getPath());
        } else {
          List<SqlNode> args = rewriteArgs(alias, (SqrlTableMacro) fnc.getFunction());
          builder.scanFunction(fnc, args);
        }
      } else if (isSelf) {
        pathWalker.setPath(context.getCurrentPath());
        if (materializeSelf || !input.hasNext()) {//treat self as a table
          builder.scanNestedTable(context.getCurrentPath());
          if (isNested) {
            RelOptTable table = planner.getCatalogReader().getSqrlTable(pathWalker.getAbsolutePath());
            pullupColumns = IntStream.range(0, table.getKeys().get(0).asSet().size())
                .mapToObj(i -> "__" + table.getRowType().getFieldList().get(i).getName() + "$pk$" + pkId.incrementAndGet())
                .collect(Collectors.toList());
          }
        } else { //treat self as a parameterized binding to the next function
          item = input.next();
          String nextIdentifier = getIdentifier(item)
              .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));
          pathWalker.walk(nextIdentifier);

          SqlUserDefinedTableFunction fnc = planner.getTableFunction(pathWalker.getAbsolutePath());
          if (fnc == null) {
            builder.scanNestedTable(pathWalker.getPath());
          } else {
            RelDataType type = planner.getCatalogReader().getSqrlTable(context.currentPath)
                .getRowType();
            List<SqlNode> args = rewriteArgs(ReservedName.SELF_IDENTIFIER.getCanonical(),
                (SqrlTableMacro) fnc.getFunction());

            builder.scanFunction(fnc, args);
          }
        }
      } else {
        throw new RuntimeException("Unknown table: " + item);
      }

      while (input.hasNext()) {
        item = input.next();
        String nextIdentifier = getIdentifier(item)
            .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));
        pathWalker.walk(nextIdentifier);

        String alias = builder.getLatestAlias();
        SqlUserDefinedTableFunction fnc = planner.getTableFunction(pathWalker.getPath());
        if (fnc == null) {
          builder.scanNestedTable(pathWalker.getPath());
        } else {
          List<SqlNode> args = rewriteArgs(alias,
              fnc.getFunction());

          builder.scanFunction(fnc, args)
              .joinLateral();
        }
      }

      SqlNode sqlNode;
      if (node.getItems().size() == 1 && pullupColumns.isEmpty()) {
        sqlNode = builder.build();
      } else {
        sqlNode = builder.buildAndProjectLast(pullupColumns);
      }

      return new Result(sqlNode, pathWalker.getAbsolutePath(), pullupColumns, List.of(),Optional.empty());
    }

    private List<SqlNode> rewriteArgs(String alias, TableFunction function) {
      //if arg needs to by a dynamic expression, rewrite.
      List<SqlNode> nodes = new ArrayList<>();
      for (FunctionParameter parameter : function.getParameters()) {
        SqrlFunctionParameter p = (SqrlFunctionParameter) parameter;
        SqlIdentifier identifier = new SqlIdentifier(List.of(alias, p.getName()),
            SqlParserPos.ZERO);
        nodes.add(identifier);
      }
      return nodes;

//
//      return function.getParameters().stream()
//          .filter(f->f instanceof SqrlFunctionParameter)
//          .map(f->(SqrlFunctionParameter)f)
//          //todo: alias name not correct, look at default value and extract dynamic param with correct field name
//          // but this is okay for now
//          .map(f->new SqlIdentifier(List.of(alias, f.getName()), SqlParserPos.ZERO))
//          .collect(Collectors.toList());
    }

    private Optional<String> getIdentifier(SqlNode item) {
      if (item instanceof SqlIdentifier) {
        return Optional.of(((SqlIdentifier) item).getSimple());
      } else if (item instanceof SqlCall) {
        return Optional.of(((SqlCall) item).getOperator().getName());
      }

      return Optional.empty();
    }

    @Override
    public Result visitJoin(SqlJoin call, Context context) {
      //Check if we should skip the lhs, if it's self and we don't materialize and there is no condition
      if (isSelfTable(call.getLeft()) && !context.isMaterializeSelf() && (call.getCondition() == null ||
          call.getCondition() instanceof SqlLiteral && ((SqlLiteral) call.getCondition()).getValue() == Boolean.TRUE)) {
        return SqlNodeVisitor.accept(this, call.getRight(), context);
      }

      Result leftNode = SqlNodeVisitor.accept(this, call.getLeft(), context);
      Context context1 = new Context(false, leftNode.currentPath,  context.aliasPathMap, false,
          false, false);
      Result rightNode = SqlNodeVisitor.accept(this, call.getRight(), context1);

      SqlNode join = new SqlJoinBuilder(call)
          .rewriteExpressions(new WalkExpressions(planner, context))
          .setLeft(leftNode.getSqlNode())
          .setRight(rightNode.getSqlNode())
          .lateral()
          .build();

      return new Result(join, rightNode.getCurrentPath(), leftNode.keysToPullUp, List.of(), Optional.empty());
    }

    @Override
    public Result visitSetOperation(SqlCall node, Context context) {
      return new Result(
          node.getOperator().createCall(node.getParserPosition(),
              node.getOperandList().stream()
                  .map(o->SqlNodeVisitor.accept(this, o, context).getSqlNode())
                  .collect(Collectors.toList())),
          List.of(),
          List.of(),
          List.of(),
          Optional.empty());
    }
  }

  @AllArgsConstructor
  public class WalkExpressions extends SqlShuttle {
    QueryPlanner planner;
    Context context;
    @Override
    public SqlNode visit(SqlCall call) {
      if (call.getKind() == SqlKind.SELECT) {
        SqrlToSql sqrlToSql = new SqrlToSql();
        Result result = sqrlToSql.rewrite(call, false, context.currentPath);

        return result.getSqlNode();
      }

      return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (validator.getDynamicParam().get(id) != null) {
        SqlDynamicParam dynamicParam = validator.getDynamicParam().get(id);
        return dynamicParam;
      }

      Preconditions.checkState(!isVariable(id.names), "Found variable when expecting one.");
      return super.visit(id);
    }
  }
  @Value
  public static class Result {
    SqlNode sqlNode;
    List<String> currentPath;
    List<String> keysToPullUp;
    List<List<String>> tableReferences;
    Optional<SqlNode> wrapperNode;
  }

  @Value
  public static class Context {
    //unbound replaces @ with system args, bound expands @ to table.
    boolean materializeSelf;
    List<String> currentPath;
    Map<String, List<String>> aliasPathMap;
    public boolean isAggregating;
    public boolean isNested;
    public boolean isLimit;

    public void addAlias(String alias, List<String> currentPath) {
      aliasPathMap.put(alias, currentPath);
    }

    public boolean hasAlias(String alias) {
      return aliasPathMap.containsKey(alias);
    }

    public List<String> getAliasPath(String alias) {
      return new ArrayList<>(getAliasPathMap().get(alias));
    }
  }
}
