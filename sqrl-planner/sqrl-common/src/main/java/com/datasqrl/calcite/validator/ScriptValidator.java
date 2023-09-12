package com.datasqrl.calcite.validator;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.ModifiableSqrlTable;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.schema.PathWalker;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlAliasCallBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlJoinBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.calcite.visitor.SqlRelationVisitor;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.SqlNameUtil;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlAssignTimestamp;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlCompoundIdentifier;
import org.apache.calcite.sql.SqrlDistinctQuery;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.calcite.sql.SqrlFromQuery;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlJoinQuery;
import org.apache.calcite.sql.SqrlSqlQuery;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.StatementVisitor;
import org.apache.calcite.sql.SqrlStreamQuery;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Graphql: sort of like inference but just all tokens
 *
 * Run the whole planner and validator w/ starwars schema.
 * Dump out all queries to snapshot: flink, postgres, and server
 *
 * Search(@text: String) :=
 * SELECT * FROM Human UNION ALL SELECT * FROM Droid UNION ALL SELECT * FROM Starship;
 * -- where textsearch(@text, coalesce(cols))
 *
 * Character := SELECT * FROM Human UNION ALL SELECT * FROM Droid;
 *
 * hero(@episode: String) :=
 * SELECT * FROM Character
 * WHERE name = 'luke' AND @episode = 'NEWHOPE'
 *    OR name = 'r2d2' AND @episode != 'NEWHOPE';
 *
 *
 */
@AllArgsConstructor
@Getter
public class ScriptValidator implements StatementVisitor<Void, Void> {
  private final SqrlFramework framework;
  private final QueryPlanner planner;
  private final ModuleLoader moduleLoader;
  private final ErrorCollector errorCollector;
  private final SqlNameUtil nameUtil;

  private final Map<SqrlImportDefinition, List<QualifiedImport>> importOps = new HashMap<>();
  private final Map<SqrlExportDefinition, QualifiedExport> exportOps = new HashMap<>();

  private final Map<SqlNode, Object> schemaTable = new HashMap<>();
  private final AtomicInteger uniqueId = new AtomicInteger(0);

  @Override
  public Void visit(SqrlImportDefinition node, Void context) {
    // IMPORT data.* AS x;
    if (node.getImportPath().isStar() && node.getAlias().isPresent()) {
      errorCollector.exception(ErrorCode.IMPORT_CANNOT_BE_ALIASED, "Import cannot be aliased");

      //Strip alias and continue to process
      node = node.clone(node.getImportPath(), Optional.empty());
    }

    NamePath path = nameUtil.toNamePath(node.getImportPath().names);
    Optional<SqrlModule> moduleOpt = moduleLoader.getModule(path.popLast());

    if (moduleOpt.isEmpty()) {
      errorCollector.exception(ErrorCode.GENERIC, "Could not find module [%s]", path);
      return null; //end processing
    }

    SqrlModule module = moduleOpt.get();

    if (path.getLast().equals(ReservedName.ALL)) {
      importOps.put(node, module.getNamespaceObjects().stream()
          .map(i->new QualifiedImport(i, Optional.empty()))
          .collect(Collectors.toList()));
      if (module.getNamespaceObjects().isEmpty()) {
        errorCollector.warn(ErrorLabel.GENERIC, "Module is empty: %s", path);
      }

      List<NamespaceObject> objects = new ArrayList<>(module.getNamespaceObjects());

      for (NamespaceObject obj : objects) {
        handleImport(Optional.of(obj), Optional.empty(), path);
      }
    } else {
      // Get the namespace object specified in the import statement
      Optional<NamespaceObject> objOpt = module.getNamespaceObject(path.getLast());

      //Keep original casing
      String objectName = node.getAlias()
          .map(a->a.names.get(0))
          .orElse(path.getLast().getDisplay());

      handleImport(objOpt, Optional.of(objectName), path);

      if (objOpt.isPresent()) {
        importOps.put(node, List.of(new QualifiedImport(objOpt.get(), Optional.of(objectName))));
      }
    }

    return null;
  }

  private void handleImport(Optional<NamespaceObject> obj, Optional<String> alias, NamePath path) {
    if (obj.isEmpty()) {
      errorCollector.exception(ErrorLabel.GENERIC,"Could not find object: [%s] %s", path);
    }
  }

  @Value
  public class QualifiedImport {
    NamespaceObject object;
    Optional<String> alias;
  }

  /**
   * Edge cases:
   * LHS not a table
   * RHS not a sink
   * Resolve all field names for export
   */
  @Override
  public Void visit(SqrlExportDefinition statement, Void context) {
    RelOptTable table = framework.getCatalogReader()
        .getSqrlTable(statement.getIdentifier().names);

    if (table == null) {
      errorCollector.exception(ErrorCode.GENERIC,
          "Could not find export table: %s", statement.getIdentifier().names);
    }

    Optional<TableSink> sink = LoaderUtil.loadSinkOpt(
        nameUtil.toNamePath(statement.getSinkPath().names), errorCollector,
        moduleLoader);

    if (sink.isEmpty()) {
      errorCollector.exception(ErrorCode.CANNOT_RESOLVE_TABLESINK,
          "Cannot resolve table sink: %s", statement.getSinkPath().names);
    }

    exportOps.put(statement, new QualifiedExport(statement.getIdentifier().names, sink.get()));

    return null;
  }

  @Value
  public class QualifiedExport {
    List<String> table;
    TableSink sink;
  }

  /**
   * Same as validate query
   * No args
   */
  @Override
  public Void visit(SqrlStreamQuery statement, Void context) {

    return null;
  }

  /**
   * lhs must exist and be assignable
   * Cannot be aggregating
   *
   * Warn if args are not used in query
   * Warn if AS is used to name an expression
   *
   * Cannot shadow a table's primary key
   *
   */
  @Override
  public Void visit(SqrlExpressionQuery statement, Void context) {
    return null;
  }

  /**
   * Rewrite table to be plannable: Take the table def and convert it to a function w/ lateral joins
   * We could retain this query and convert it back to sql for later processing. We can then expand
   * the function in a simpler way.
   *
   * Can be plannable
   * Can be assignable
   * If nested, has a '@' table as the first table
   * Validate args
   *
   * Find all SELECT * on direct tables, these are possible Aliases.
   *
   * Assure columns are named and no collisions.
   *
   * We need a global PK incrementer to allow for multiple select * and have it still work (UUID collisions)
   *
   * If it errors completely:
   * Derive type field names, give ANY
   *
   * Allow all function resolution during validation, no need to plan, give any type
   *
   * Allow UNION access tables
   *
   */
  @Override
  public Void visit(SqrlSqlQuery statement, Void context) {


    validateTable(statement, statement.getQuery());

    return null;
  }

  public RelDataType validateTable(SqrlAssignment statement, SqlNode query) {
    SqlValidator validator = planner.createSqlValidator();

    SqlNode sqlNode;
    try {
      SqrlToSql sqrlToSql = new SqrlToSql(planner);
      List<String> parent = List.of();
      if (statement.getIdentifier().names.size() > 1) {
        parent = SqrlListUtil.popLast(statement.getIdentifier().names);
        RelOptTable sqrlTable = planner.getCatalogReader().getSqrlTable(parent);
        if (sqrlTable == null) {
          errorCollector.exception(ErrorLabel.GENERIC, "Could not find parent assignment table: %s", parent);
        }
      }

      Result result = sqrlToSql.rewrite(query, parent, statement.getTableArgs().orElseGet(()->
          new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of())));
      sqlNode = result.getSqlNode();
      System.out.println(sqlNode);
      framework.getSqrlOperatorTable()
              .addPlanningFnc(result.getFncs());

      validator.validate(result.getSqlNode());
    } catch (Exception e) {
      e.printStackTrace();
      errorCollector.exception(ErrorLabel.GENERIC, e.getMessage());
      return null;
    }

    //we have enough to plan
    RelDataType type =
        validator.getValidatedNodeType(sqlNode);

    System.out.println(type);
    System.out.println("Validation: "+sqlNode);
    try {
      RelNode relNode = planner.plan(Dialect.CALCITE, sqlNode);
      System.out.println("Validation: "+relNode.explain());
      type = relNode.getRowType();
      return type;
    } catch (Exception e) {
      errorCollector.exception(ErrorLabel.GENERIC, e.getMessage());
    }

    System.out.println(type);
    return type;
  }

  @AllArgsConstructor
  public class SqrlToSql implements SqlRelationVisitor<Result, Context> {
    final QueryPlanner planner;
    private final List<SqlFunction> fncs = new ArrayList<>();

    public Result rewrite(SqlNode query, List<String> currentPath, SqrlTableFunctionDef tableArgs) {
      Context context = new Context(currentPath, new HashMap<>(), tableArgs);

      return SqlNodeVisitor.accept(this, query, context);
    }

    @Override
    public Result visitQuerySpecification(SqlSelect call, Context context) {
      // Copy query specification with new RelNode.
      Context newContext = new Context(context.currentPath, new HashMap<>(), context.tableFunctionDef);
      Result result = SqlNodeVisitor.accept(this, call.getFrom(), newContext);

      // Todo: check distinct rules

      SqlSelectBuilder select = new SqlSelectBuilder(call)
          .setFrom(result.getSqlNode())
          .rewriteExpressions(new WalkSubqueries(planner, newContext));

      return new Result(select.build(), result.getCurrentPath(), result.getFncs());
    }

    @Override
    public Result visitAliasedRelation(SqlCall node, Context context) {
      Result result = SqlNodeVisitor.accept(this, node.getOperandList().get(0), context);
      SqlAliasCallBuilder aliasBuilder = new SqlAliasCallBuilder(node);

      context.addAlias(aliasBuilder.getAlias(), result.getCurrentPath());

      SqlNode newNode = aliasBuilder.setTable(result.getSqlNode())
          .build();

      return new Result(newNode, result.getCurrentPath(), result.getFncs());
    }

    @Override
    public Result visitTable(SqrlCompoundIdentifier node, Context context) {
      Iterator<SqlNode> input = node.getItems().iterator();
      PathWalker pathWalker = new PathWalker(planner);

      SqlNode item = input.next();

      String identifier = getIdentifier(item)
          .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));

      boolean isAlias = context.hasAlias(identifier);
      boolean isSelf = identifier.equals(ReservedName.SELF_IDENTIFIER.getCanonical());
      boolean isSchemaTable = getIdentifier(item)
          .map(i->planner.getCatalogReader().getSqrlTable(List.of(i)) != null)
          .orElse(false);

      RelDataType latestTable;
      if (isSchemaTable) {
        pathWalker.walk(identifier);
        RelOptTable table = planner.getCatalogReader().getSqrlTable(pathWalker.getPath());
        if (table == null) {
          throw errorCollector.exception(ErrorLabel.GENERIC, "Could not find path: %s",
              pathWalker.getUserDefined());
        }
        latestTable = table.getRowType();
      } else if (isAlias) {
        if (!input.hasNext()) {
          throw errorCollector.exception(ErrorLabel.GENERIC, "Alias by itself.");
        }
        pathWalker.setPath(context.getAliasPath(identifier));
        //Walk the next one and push in table function
        item = input.next();


        Optional<String> nextIdentifier = getIdentifier(item);
        if (nextIdentifier.isEmpty()) {
          throw errorCollector.exception(ErrorLabel.GENERIC,
              "Table is not a valid identifier");
        }

        pathWalker.walk(nextIdentifier.get());
        //get table of current path (no args)
        RelOptTable table = planner.getCatalogReader().getSqrlTable(pathWalker.getPath());

        if (table == null) {
          throw errorCollector.exception(ErrorLabel.GENERIC, "Could not find path: %s",
              pathWalker.getUserDefined());
        }

        latestTable = table.getRowType();
      } else if (isSelf) {
        pathWalker.setPath(context.getCurrentPath());
        if (!input.hasNext()) {//treat self as a table
          RelOptTable table = planner.getCatalogReader().getSqrlTable(context.getCurrentPath());
          if (table == null) {
            throw errorCollector.exception(ErrorLabel.GENERIC, "Could not find parent table: %s",
                pathWalker.getUserDefined());
          }
          latestTable = table.getRowType();
        } else { //treat self as a parameterized binding to the next function
          item = input.next();
          Optional<String> nextIdentifier = getIdentifier(item);
          if (nextIdentifier.isEmpty()) {
            throw errorCollector.exception(ErrorLabel.GENERIC,
                "Table is not a valid identifier");
          }
          pathWalker.walk(nextIdentifier.get());

          RelOptTable table = planner.getCatalogReader().getSqrlTable(pathWalker.getPath());
          if (table == null) {
            throw errorCollector.exception(ErrorLabel.GENERIC, "Could not find table: %s", pathWalker.getUserDefined());
          }
          latestTable = table.getRowType();
        }
      } else {
        throw errorCollector.exception(ErrorLabel.GENERIC, "Could not find table: %s",
            identifier);
      }

      System.out.println(latestTable.getFieldNames());
      while (input.hasNext()) {
        item = input.next();
        Optional<String> nextIdentifier = getIdentifier(item);
        if (nextIdentifier.isEmpty()) {
          throw errorCollector.exception(ErrorLabel.GENERIC,
              "Table is not a valid identifier");
        }
        pathWalker.walk(nextIdentifier.get());

        RelOptTable table = planner.getCatalogReader().getSqrlTable(pathWalker.getPath());
        if (table == null) {
          throw errorCollector.exception(ErrorLabel.GENERIC, "Could not find table: %s", pathWalker.getUserDefined());
        }
        latestTable = table.getRowType();
        System.out.println(latestTable.getFieldNames());

      }

      final RelDataType latestTable2 = latestTable;

      TableFunction function = new TableFunction() {
        @Override
        public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
          return latestTable2;
        }

        @Override
        public Type getElementType(List<Object> list) {
          return Object.class;
        }

        @Override
        public List<FunctionParameter> getParameters() {
          return List.of();
        }
      };
      SqlUserDefinedTableFunction fnc = new SqlUserDefinedTableFunction(
          new SqlIdentifier(node.getDisplay() + "$validate$"
              + uniqueId.incrementAndGet(), SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION,
          sqlOperatorBinding -> latestTable2, null, new SqlOperandMetadata() {
        @Override
        public List<RelDataType> paramTypes(RelDataTypeFactory relDataTypeFactory) {
          return List.of();
        }

        @Override
        public List<String> paramNames() {
          return List.of();
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding sqlCallBinding, boolean b) {
          return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
          return new SqlOperandCountRange() {

            @Override
            public boolean isValidCount(int i) {
              return true;
            }

            @Override
            public int getMin() {
              return 0;
            }

            @Override
            public int getMax() {
              return Integer.MAX_VALUE;
            }
          };
        }

        @Override
        public String getAllowedSignatures(SqlOperator sqlOperator, String s) {
          return null;
        }

        @Override
        public Consistency getConsistency() {
          return null;
        }

        @Override
        public boolean isOptional(int i) {
          return true;
        }
      }, function
      );

      List<SqlNode> args = node.getItems().stream()
          .filter(f-> f instanceof SqlCall)
          .map(f-> (SqlCall)f)
          .flatMap(f->f.getOperandList().stream())
          .collect(Collectors.toList());

      SqlCall call = fnc.createCall(SqlParserPos.ZERO, args);
      this.fncs.add(fnc);

      SqlCall call1 = SqlStdOperatorTable.COLLECTION_TABLE.createCall(SqlParserPos.ZERO, call);
      return new Result(call1, pathWalker.getAbsolutePath(), fncs);
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
    public Result visitJoin(
        SqlJoin call, Context context) {
      Result leftNode = SqlNodeVisitor.accept(this, call.getLeft(), context);

      Context context1 = new Context(leftNode.currentPath,  context.aliasPathMap, context.tableFunctionDef);
      Result rightNode = SqlNodeVisitor.accept(this, call.getRight(), context1);

      SqlNode join = new SqlJoinBuilder(call)
          .rewriteExpressions(new WalkSubqueries(planner, context))
          .setLeft(leftNode.getSqlNode())
          .setRight(rightNode.getSqlNode())
          .lateral()
          .build();

      return new Result(join, rightNode.getCurrentPath(), fncs);
    }

    @Override
    public Result visitSetOperation(SqlCall node, Context context) {
      return new Result(
          node.getOperator().createCall(node.getParserPosition(),
              node.getOperandList().stream()
                  .map(o->SqlNodeVisitor.accept(this, o, context).getSqlNode())
                  .collect(Collectors.toList())),
          List.of(), fncs);
    }

  }

  @Value
  public class Result {
    SqlNode sqlNode;
    List<String> currentPath;
    List<SqlFunction> fncs;
  }

  @Value
  public class Context {
    //unbound replaces @ with system args, bound expands @ to table.
    List<String> currentPath;
    Map<String, List<String>> aliasPathMap;
    SqrlTableFunctionDef tableFunctionDef;

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

  @AllArgsConstructor
  public class WalkSubqueries extends SqlShuttle {
    QueryPlanner planner;
    Context context;
    @Override
    public SqlNode visit(SqlCall call) {
      if (call.getKind() == SqlKind.SELECT) {
        SqrlToSql sqrlToSql = new SqrlToSql(planner);
        Result result = sqrlToSql.rewrite(call, context.currentPath, context.tableFunctionDef);

        return result.getSqlNode();
      }

      return super.visit(call);
    }
  }

  /**
   * if JOIN, it should be nested. if FROM, can be anywhere
   *
   * Append an alias.* to the end so we can use the same logic to determine
   * what it points to.
   *
   */
  @Override
  public Void visit(SqrlJoinQuery statement, Void context) {

    /**
     * Add the implicit table join as sql since the fields are accessible
     */
    SqlJoinBuilder joinBuilder = new SqlJoinBuilder();
    joinBuilder.setLeft(new SqrlCompoundIdentifier(SqlParserPos.ZERO,
        List.of(new SqlIdentifier("@", SqlParserPos.ZERO))));
    joinBuilder.setRight(statement.getQuery().getFrom());

    SqlSelectBuilder sqlSelectBuilder = new SqlSelectBuilder(statement.getQuery());
    sqlSelectBuilder.setFrom(joinBuilder.build());

//    RelDataType type = validateTable(statement, sqlSelectBuilder.build());

    return null;
  }

  @AllArgsConstructor
  public static class ViewTable extends AbstractTable implements ModifiableSqrlTable {
    RelDataType type;
    @Override
    public void addColumn(String name, RexNode column, RelDataTypeFactory typeFactory) {

    }

    @Override
    public SQRLTable getSqrlTable() {
      return null;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
      return type;
    }
  }

  @Override
  public Void visit(SqrlFromQuery statement, Void context) {
    return null;
  }

  /**
   * Validate as query, cannot be nested (probably can be but disallow). No joins.
   *
   * Must have Order by statement
   */
  @Override
  public Void visit(SqrlDistinctQuery statement, Void context) {
    return null;
  }

  /**
   * Warning cases:
   * IMPORT t TIMESTAMP expression <- should have alias
   */
  @Override
  public Void visit(SqrlAssignTimestamp statement, Void context) {
    return null;
  }

  public void validate(ScriptNode script) {
    for (SqlNode sqlNode : script.getStatements()) {
      validateStatement((SqrlStatement)sqlNode);
    }
  }

  public void validateStatement(SqrlStatement sqlNode) {
    SqlNodeVisitor.accept(this, sqlNode, null);
  }
}
