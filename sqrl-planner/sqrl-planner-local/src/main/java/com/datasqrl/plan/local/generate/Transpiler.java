package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.SqlValidatorUtil;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.plan.local.transpile.AddContextFields;
import com.datasqrl.plan.local.transpile.AddHints;
import com.datasqrl.plan.local.transpile.AnalyzeStatement;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.DistinctAssignment;
import org.apache.calcite.sql.ExpressionAssignment;
import org.apache.calcite.sql.ImportDefinition;
import org.apache.calcite.sql.JoinAssignment;
import org.apache.calcite.sql.QueryAssignment;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.StreamAssignment;
import org.apache.calcite.sql.validate.SqlValidator;

public class Transpiler {

  public SqlNode transpile(SqrlStatement query, Namespace ns, ErrorCollector errors) {
    try {
      return transpileHelper(query, ns, errors);
    } catch (Exception e) {
      throw errors.handle(e);
    }
  }

  public SqlNode transpileHelper(SqrlStatement query, Namespace ns, ErrorCollector errors) {
    checkPathWritable(ns, query);

    Optional<SQRLTable> table = getContext(ns, query);

    Optional<VirtualRelationalTable> context = table.map(SQRLTable::getVt);

    SqlTransformer transformer = createTransformer(query, ns.getSchema(), table, ns);
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

  private SqlTransformer createTransformer(SqrlStatement query, SqrlSchema schema, Optional<SQRLTable> table,
      Namespace ns) {
    List<String> assignmentPath = getAssignmentPath(query);
    Function<SqlNode, Analysis> analyzer = (node) -> new AnalyzeStatement(schema, assignmentPath, table, ns)
        .accept(node);
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
      ExpressionAssignment expr = (ExpressionAssignment) query;
      SqlNode sqlNode = expr.getExpression();
      if (context.isEmpty()) {
        throw new SqrlAstException(ErrorCode.MISSING_DEST_TABLE, query.getParserPosition(),
            String.format("Could not find table: %s", query.getNamePath()));
      }
      return transformExpressionToQuery(sqlNode, Optional.of(expr.getNamePath().getLast()));
    } else if (query instanceof ImportDefinition) {
      ConvertTimestampToExpression tsToQuery = new ConvertTimestampToExpression();
      SqlNode ts = tsToQuery.convert((ImportDefinition)query);
      return transformExpressionToQuery(ts, Optional.empty());
    }
    throw new IllegalArgumentException("Unsupported query type: " + query.getClass().getSimpleName());
  }

  private SqlNode addContextFields(SqlValidator sqrlValidator, Optional<VirtualRelationalTable> context,
      boolean isAggregate, SqlNode sql) {
    return new AddContextFields(sqrlValidator, context, isAggregate).accept(sql);
  }

  public SqlNode convertDistinctOnToQuery(DistinctAssignment node) {
    SqlNodeFactory sqlNodeFactory = new SqlNodeFactory(node.getParserPosition());
    SqlSelect query = sqlNodeFactory.createSqlSelect();
    query.setSelectList(sqlNodeFactory.createStarSelectList());
    query.setFrom(node.getTable());
    query.setOrderBy(sqlNodeFactory.list(node.getOrder()));
    query.setFetch(SqlLiteral.createExactNumeric("1", node.getParserPosition()));
    query.setHints(sqlNodeFactory.createDistinctOnHintList(node.getPartitionKeys()));
    return query;
  }

  private Optional<SQRLTable> getContext(Namespace ns, SqrlStatement statement) {
    if (statement instanceof ImportDefinition) { //a table import
      ImportDefinition def = ((ImportDefinition) statement);

      //TODO: The logic is not fully correct here. The last name of the path isn't
      // necessarily the table name. Maybe use a qualified name?
      NamePath name = def.getAlias()
          .map(a-> Name.system(a.names.get(0)).toNamePath()).orElse(
              statement.getNamePath().getLast().toNamePath());
      return resolveTable(ns, name, false);
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

  private void checkPathWritable(Namespace ns, SqrlStatement query) {
    if (query instanceof ImportDefinition) {
      return;
    }
    NamePath assign = query.getNamePath().popLast();
    NamePath full = query.getNamePath();

    Optional<List<Object>> assignPath = ns.getSchema().walk(assign);
    Optional<List<Object>> fullPath = ns.getSchema().walk(full);

    //always writable
    if (full.size() == 1) {
      return;
    }

    if (assignPath.isPresent()) {
      //Assure we can write to it (no JOIN or PARENT rels)
      List<Object> p = assignPath.get();
      Optional<Object> hasJoin = p.stream()
          .filter(f -> f instanceof Relationship && (
              ((Relationship) f).getJoinType() == Relationship.JoinType.JOIN
                  || ((Relationship) f).getJoinType() == Relationship.JoinType.PARENT))
          .findAny();
      if (hasJoin.isPresent()) {
        throw new SqrlAstException(ErrorCode.PATH_NOT_WRITABLE, query.getParserPosition(),
            "Assignment path contains a join declaration or a reference to parent. [%s]", full.getDisplay());
      }
    }

    if (assignPath.isEmpty()) {
      if (full.size() == 1) {
        throw new SqrlAstException(ErrorCode.MISSING_DEST_TABLE,
            query.getParserPosition(),
            "Base relation does not exist [%s]. Cannot assign statement to a missing relation.",
            full.getNames()[0].getDisplay());
      } else {
        throw new SqrlAstException(ErrorCode.MISSING_TABLE, query.getParserPosition(),
            "Could not find table path [%s]", assign.getDisplay());
      }
    }

    //check to see if we're shadowing a relationship
    if (fullPath.isPresent() && fullPath.get().get(fullPath.get().size() - 1) instanceof Relationship) {
        throw new SqrlAstException(ErrorCode.CANNOT_SHADOW_RELATIONSHIP, query.getParserPosition(),
            "Attempting to shadow relationship [%s]", full.getDisplay());
    }
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

  private SqlNode transformExpressionToQuery(SqlNode sqlNode, Optional<Name> name) {
    SqlNodeFactory factory = new SqlNodeFactory(sqlNode.getParserPosition());
    SqlSelect select = factory.createSqlSelect();
    select.setSelectList(name.map(n->factory.list(factory.callAs(sqlNode, n.getDisplay())))
        .orElseGet(()->factory.list(sqlNode)));
    return select;
  }
}
