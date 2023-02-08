package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.calcite.SqlValidatorUtil;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import com.datasqrl.plan.local.transpile.AnalyzeStatement;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
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
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.StreamAssignment;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import com.datasqrl.plan.local.transpile.*;

public class Transpiler {

  private final SystemContext systemContext;

  public Transpiler(SystemContext systemContext) {

    this.systemContext = systemContext;
  }
  public SqlNode transpile(SqrlStatement query, Namespace ns) {
    Optional<SQRLTable> table = getContext(ns, query);
    table.ifPresent(t -> checkPathWritable(ns, query.getNamePath().popLast()));
    Optional<VirtualRelationalTable> context = table.map(SQRLTable::getVt);

    SqlTransformer transformer = createTransformer(query, ns.getSchema(), table);
    SqlNode node = convertToQuery(query, context);
    node = transformer.transform(node);

    return postprocess(query, ns, context, node);
  }

  public SqlNode postprocess(SqrlStatement query, Namespace ns,
      Optional<VirtualRelationalTable> context, SqlNode node) {
    if (query instanceof DistinctAssignment) {
      SqlValidator sqrlValidator = createValidator(ns);
      sqrlValidator.validate(node);
      new AddHints(sqrlValidator, context).accept(true, node);
      sqrlValidator.validate(node);
      return node;
    } else if (query instanceof JoinAssignment) {
      return node;
    } else {
      SqlValidator sqrlValidator = createValidator(ns);
      sqrlValidator.validate(node);
      SqlNode rewritten = addContextFields(sqrlValidator, context, isAggregate(sqrlValidator, node),
          node);
      SqlValidator prevalidate = createValidator(ns);
      prevalidate.validate(rewritten);
      new AddHints(prevalidate, context).accept(query instanceof DistinctAssignment, rewritten);
      SqlValidator validator = createValidator(ns);
      return validator.validate(rewritten);
    }
  }

  private SqlTransformer createTransformer(SqrlStatement query, SqrlCalciteSchema schema, Optional<SQRLTable> table) {
    List<String> assignmentPath = getAssignmentPath(query);
    Function<SqlNode, Analysis> analyzer = (node) -> new AnalyzeStatement(schema, assignmentPath, table).accept(node);
    return SqlTransformerFactory.create(analyzer, table.isPresent());
  }

  // Helper functions
  private List<String> getAssignmentPath(SqrlStatement query) {
    return query.getNamePath().popLast()
        .stream()
        .map(e -> e.getCanonical())
        .collect(Collectors.toList());
  }

  private SqlNode convertToQuery(SqrlStatement query,
      Optional<VirtualRelationalTable> context) {
    if (query instanceof DistinctAssignment) {
      return convertDistinctOnToQuery((DistinctAssignment) query);
    } else if (query instanceof StreamAssignment) {
      return ((StreamAssignment) query).getQuery();
    } else if (query instanceof QueryAssignment) {
      return ((QueryAssignment) query).getQuery();
    } else if (query instanceof JoinAssignment) {
      return ((JoinAssignment) query).getQuery();
    } else if (query instanceof ExpressionAssignment) {
      SqlNode sqlNode = ((ExpressionAssignment) query).getExpression();
      if (context.isEmpty()) {
        throw new SqrlAstException(ErrorCode.MISSING_DEST_TABLE, query.getParserPosition(),
            String.format("Could not find table: %s", query.getNamePath()));
      }
      return transformExpressionToQuery(sqlNode);
    } else if (query instanceof ImportDefinition) {
      ConvertTimestampToQuery tsToQuery = new ConvertTimestampToQuery();
      return tsToQuery.convert((ImportDefinition)query);
    }
    throw new IllegalArgumentException("Unsupported query type: " + query.getClass().getSimpleName());
  }

  private SqlNode addContextFields(SqlValidator sqrlValidator, Optional<VirtualRelationalTable> context,
      boolean isAggregate, SqlNode sql) {
    return new AddContextFields(sqrlValidator, context, isAggregate).accept(sql);
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
  private Optional<SQRLTable> getContext(Namespace ns, SqrlStatement statement) {
    if (statement instanceof ImportDefinition) { //a table import
      return resolveTable(ns,
          ((ImportDefinition) statement).getAlias()
              .map(a-> Name.system(a.names.get(0)).toNamePath()).orElse(
              statement.getNamePath().getLast().toNamePath()), false);
    }
    return resolveTable(ns, statement.getNamePath(), true);
  }

  private Optional<SQRLTable> resolveTable(Namespace ns, NamePath namePath, boolean getParent) {
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
  private void checkPathWritable(Namespace ns, NamePath path) {
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
  private SqlValidator createValidator(Namespace ns) {
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
}
