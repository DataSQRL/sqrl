package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.calcite.SqlValidatorUtil;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import com.datasqrl.plan.local.transpile.AddContextTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import com.datasqrl.plan.local.transpile.ConvertJoinDeclaration;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.sql.DistinctAssignment;
import org.apache.calcite.sql.ExpressionAssignment;
import org.apache.calcite.sql.ImportDefinition;
import org.apache.calcite.sql.JoinAssignment;
import org.apache.calcite.sql.QueryAssignment;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.StreamAssignment;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import com.datasqrl.plan.local.transpile.*;

public class Transpiler {

  private final SystemContext systemContext;

  public Transpiler(SystemContext systemContext) {

    this.systemContext = systemContext;
  }

  public SqlNode transpile(SqrlStatement query, FlinkNamespace ns) {
    List<String> assignmentPath = query.getNamePath().popLast()
        .stream()
        .map(e -> e.getCanonical())
        .collect(Collectors.toList());

    Optional<SQRLTable> table = getContext(ns, query);
    table.ifPresent(
        t -> checkPathWritable(ns, query, query.getNamePath().popLast()));
    Optional<VirtualRelationalTable> context =
        table.map(t -> t.getVt());

    Function<SqlNode, Analysis> analyzer = (node) -> new AnalyzeStatement(ns.getSchema(),
        assignmentPath, table)
        .accept(node);

    SqlNode node = query;
    if (query instanceof DistinctAssignment) {
      node = convertDistinctOnToQuery((DistinctAssignment) query);
    } else if (query instanceof QueryAssignment) {
      node = ((QueryAssignment) query).getQuery();
    } else if (query instanceof JoinAssignment) {
      node = ((JoinAssignment) query).getQuery();
    } else if (query instanceof StreamAssignment) {
      node = ((StreamAssignment) query).getQuery();
    } else if (query instanceof ExpressionAssignment) {
      SqlNode sqlNode = ((ExpressionAssignment) query).getExpression();
      node = transformExpressionToQuery(sqlNode);
    } else if (query instanceof ImportDefinition) {
      ConvertTimestampToQuery tsToQuery = new ConvertTimestampToQuery();
      node = tsToQuery.convert((ImportDefinition)query);
    }
    Analysis currentAnalysis = null;

    List<Function<Analysis, SqlShuttle>> transforms = List.of(
        (analysis) -> new AddContextTable(table.isPresent()),
        QualifyIdentifiers::new,
        FlattenFieldPaths::new,
        FlattenTablePaths::new,
        ReplaceWithVirtualTable::new,
        AllowMixedFieldUnions::new
    );

    for (Function<Analysis, SqlShuttle> transform : transforms) {
      node = node.accept(transform.apply(currentAnalysis));
      currentAnalysis = analyzer.apply(node);
    }


    if (query instanceof DistinctAssignment) {
      SqlNode sql = node;
      SqlValidator sqrlValidator = createValidator(ns);

      sqrlValidator.validate(sql);

//      checkState(context.isEmpty(), ErrorCode.NESTED_DISTINCT_ON, op.getStatement());
      new AddHints(sqrlValidator, context).accept(true, sql);

      SqlValidator validate2 = createValidator(ns);
      validate2.validate(sql);
      return sql;
    } else if (query instanceof JoinAssignment) {
      return node;
    } else {
      SqlNode sql = node;
      SqlValidator sqrlValidator = createValidator(ns);

      sqrlValidator.validate(sql);

      SqlNode rewritten = new AddContextFields(sqrlValidator, context,
          isAggregate(sqrlValidator, sql)).accept(sql);

      SqlValidator prevalidate = createValidator(ns);
      prevalidate.validate(rewritten);
      new AddHints(prevalidate, context).accept(query instanceof DistinctAssignment, rewritten);

      SqlValidator validator = createValidator(ns);
      SqlNode newNode2 = validator.validate(rewritten);
      return newNode2;
    }

  }

  public SqlNode convertDistinctOnToQuery(DistinctAssignment node) {
    SqlNode query =
        new SqlSelect(
            node.getParserPosition(),
            null,
            new SqlNodeList(List.of(SqlIdentifier.star(node.getParserPosition())), node.getParserPosition()),
            node.getTable(),
            null,
            null,
            null,
            null,
            new SqlNodeList(node.getOrder(), node.getParserPosition()),
            null,
            SqlLiteral.createExactNumeric("1", node.getParserPosition()),
            new SqlNodeList(List.of(new SqlHint(node.getParserPosition(),
                new SqlIdentifier("DISTINCT_ON", node.getParserPosition()),
                new SqlNodeList(node.getPartitionKeys(), node.getParserPosition()),
                HintOptionFormat.ID_LIST
            )), node.getParserPosition())
        );
    return query;
  }
  private Optional<SQRLTable> getContext(FlinkNamespace ns, SqrlStatement statement) {
    if (statement instanceof ImportDefinition) { //a table import
      return resolveTable(ns,
          ((ImportDefinition) statement).getAlias()
              .map(a-> Name.system(a.names.get(0)).toNamePath()).orElse(
              statement.getNamePath().getLast().toNamePath()), false);
    }
    return resolveTable(ns, statement.getNamePath(), true);
  }

  private Optional<SQRLTable> resolveTable(FlinkNamespace ns, NamePath namePath, boolean getParent) {
    if (getParent && !namePath.isEmpty()) {
      namePath = namePath.popLast();
    }
    if (namePath.isEmpty()) {
      return Optional.empty();
    }
    Optional<SQRLTable> table =
        Optional.ofNullable(ns.getSchema().getTable(namePath.get(0).getDisplay(), false))
            .map(t -> (SQRLTable) t.getTable());
    NamePath childPath = namePath.popFirst();
    return table.flatMap(t -> t.walkTable(childPath));
  }
  private void checkPathWritable(FlinkNamespace ns, SqrlStatement statement, NamePath path) {
    Optional<SQRLTable> table = Optional.ofNullable(
            ns.getSchema().getTable(path.get(0).getCanonical(), false))
        .filter(e -> e.getTable() instanceof SQRLTable)
        .map(e -> (SQRLTable) e.getTable());
    Optional<Field> field = table
        .map(t -> t.walkField(path.popFirst()))
        .stream()
        .flatMap(f->f.stream())
        .filter(f -> f instanceof Relationship && (
            ((Relationship) f).getJoinType() == Relationship.JoinType.JOIN
                || ((Relationship) f).getJoinType() == Relationship.JoinType.PARENT))
        .findAny();
    systemContext.getErrors().
        checkFatal(field.isEmpty(), ErrorCode.PATH_CONTAINS_RELATIONSHIP,
            "Path is not writable %s", path);
  }
  private SqlValidator createValidator(FlinkNamespace ns) {
    return SqlValidatorUtil.createSqlValidator(ns.getSchema(),
        ns.getOperatorTable());
  }
  private boolean isAggregate(SqlValidator sqrlValidator, SqlNode node) {
    if (node instanceof SqlSelect) {
      return sqrlValidator.isAggregate((SqlSelect) node);
    }
    return sqrlValidator.isAggregate(node);
  }
  private SqlNode transformExpressionToQuery(SqlNode sqlNode) {
//    checkState(getContext(env, statement).isPresent(), ErrorCode.MISSING_DEST_TABLE, statement,
//        String.format("Could not find table: %s", statement.getNamePath()));
    return new SqlSelect(SqlParserPos.ZERO,
        SqlNodeList.EMPTY,
        new SqlNodeList(List.of(sqlNode), SqlParserPos.ZERO),
        null,
        null,
        null,
        null,
        SqlNodeList.EMPTY,
        null,
        null,
        null,
        SqlNodeList.EMPTY
    );
  }
//
}
