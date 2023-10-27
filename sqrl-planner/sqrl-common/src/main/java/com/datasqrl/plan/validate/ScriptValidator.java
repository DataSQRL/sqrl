package com.datasqrl.plan.validate;

import static org.apache.calcite.sql.SqlUtil.stripAs;

import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlJoinBuilder;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.calcite.visitor.SqlRelationVisitor;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.validate.SqrlToValidatorSql.Result;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.util.CheckUtil;
import com.datasqrl.util.SqlNameUtil;
import com.google.common.collect.ArrayListMultimap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlSqlValidator;
import org.apache.commons.lang3.tuple.Pair;

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
  private final Map<SqlNode, RelOptTable> tableMap = new HashMap<>();
  private final Map<SqlNode, SqlDynamicParam> dynamicParam = new HashMap<>();
  private final Map<FunctionParameter, SqlDynamicParam> paramMapping = new HashMap<>();
  private final Map<SqrlAssignment, SqlNode> preprocessSql = new HashMap<>();
  private final Map<SqrlAssignment, Boolean> isMaterializeTable = new HashMap<>();
  private final Map<SqrlAssignment, List<String>> fieldNames = new HashMap<>();
  private final ArrayListMultimap<SqlNode, Function> isA = ArrayListMultimap.create();
  private final ArrayListMultimap<SqlNode, FunctionParameter> parameters = ArrayListMultimap.create();

  private final List<SqlFunction> plannerFns = new ArrayList<>();
  
  @Override
  public Void visit(SqrlImportDefinition node, Void context) {
    // IMPORT data.* AS x;
    if (node.getImportPath().isStar() && node.getAlias().isPresent()) {
      addError(ErrorCode.IMPORT_CANNOT_BE_ALIASED,  node, "Import cannot be aliased");

      //Strip alias and continue to process
      node = node.clone(node.getImportPath(), Optional.empty());
    }

    NamePath path = nameUtil.toNamePath(node.getImportPath().names);
    Optional<SqrlModule> moduleOpt = moduleLoader.getModule(path.popLast());

    if (moduleOpt.isEmpty()) {
      addError(ErrorCode.GENERIC, node, "Could not find module [%s]", path);
      return null; //end processing
    }

    SqrlModule module = moduleOpt.get();

    if (path.getLast().equals(ReservedName.ALL)) {
      importOps.put(node, module.getNamespaceObjects().stream()
          .map(i->new QualifiedImport(i, Optional.empty()))
          .collect(Collectors.toList()));
      if (module.getNamespaceObjects().isEmpty()) {
        addWarn(ErrorLabel.GENERIC, node, "Module is empty: %s", path);
      }

      List<NamespaceObject> objects = new ArrayList<>(module.getNamespaceObjects());

      for (NamespaceObject obj : objects) {
        handleImport(node, Optional.of(obj), Optional.empty(), path);
      }
    } else {
      // Get the namespace object specified in the import statement
      Optional<NamespaceObject> objOpt = module.getNamespaceObject(path.getLast());

      //Keep original casing
      String objectName = node.getAlias()
          .map(a->a.names.get(0))
          .orElse(path.getLast().getDisplay());

      handleImport(node, objOpt, Optional.of(objectName), path);

      if (objOpt.isPresent()) {
        importOps.put(node, List.of(new QualifiedImport(objOpt.get(), Optional.of(objectName))));
      }
    }

    return null;
  }

  private void handleImport(SqrlImportDefinition node, Optional<NamespaceObject> obj, Optional<String> alias, NamePath path) {
    if (obj.isEmpty()) {
      addError(ErrorLabel.GENERIC, node.getIdentifier(), "Could not find import object: %s", path.getDisplay());
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
  public Void visit(SqrlExportDefinition node, Void context) {
    Optional<RelOptTable> table = resolveTable(node.getIdentifier(), node.getIdentifier().names);
    Optional<TableSink> sink = LoaderUtil.loadSinkOpt(nameUtil.toNamePath(node.getSinkPath().names),
        errorCollector, moduleLoader);

    if (sink.isEmpty()) {
      addError(ErrorCode.CANNOT_RESOLVE_TABLESINK, node,"Cannot resolve table sink: %s",
          nameUtil.toNamePath(node.getSinkPath().names).getDisplay());
      return null;
    }

    exportOps.put(node, new QualifiedExport(node.getIdentifier().names, sink.get()));

    return null;
  }

  @Value
  public class QualifiedExport {
    List<String> table;
    TableSink sink;
  }

  @Override
  public Void visit(SqrlAssignment assignment, Void context) {
    if (assignment.getIdentifier().names.size() > 1) {
      List<String> path = SqrlListUtil.popLast(assignment.getIdentifier().names);
      SqlUserDefinedTableFunction tableFunction = framework.getQueryPlanner()
          .getTableFunction(path);
      if (tableFunction == null) {
        throw addError(ErrorLabel.GENERIC, assignment.getIdentifier(), "Could not find table: %s", String.join(".", path));
      }
      TableFunction function = tableFunction.getFunction();
      if (function instanceof Relationship && ((Relationship) function).getJoinType() != JoinType.CHILD) {
        addError(ErrorLabel.GENERIC, assignment.getIdentifier(), "Cannot assign query to table");
      }
    }


    return null;
  }

  @Override
  public Void visit(SqrlStreamQuery node, Void context) {
    boolean materializeSelf = materializeSelfQuery(node);
    isMaterializeTable.put(node,materializeSelf);


    visit((SqrlAssignment) node, null);
    validateTable(node, node.getQuery(), node.getTableArgs(), materializeSelf);
    return null;
  }

  private boolean materializeSelfQuery(SqrlSqlQuery node) {
    //don't materialize self if we have external arguments, the query will be inlined or called from gql
    return !(node.getTableArgs().isPresent() && node.getTableArgs().get().getParameters().size() > 0) ||
            //materialize self if we have a LIMIT clause
            (node.getQuery() instanceof SqlSelect && ((SqlSelect)node.getQuery()).getFetch() != null);
  }

  @Override
  public Void visit(SqrlExpressionQuery node, Void context) {

    //If on root table, use root table
    //If on nested, use @.table
    List<String> tablePath;
    if (node.getIdentifier().names.size() > 2) {
      tablePath = List.of(ReservedName.SELF_IDENTIFIER.getCanonical(),
          node.getIdentifier().names.get(node.getIdentifier().names.size() - 2));
    } else if (node.getIdentifier().names.size() == 2) {
      tablePath = List.of(ReservedName.SELF_IDENTIFIER.getCanonical());
    } else {
      throw addError(ErrorLabel.GENERIC, node.getExpression(),
          "Cannot assign expression to root");
    }

    visit((SqrlAssignment) node, null);
    if (node.getTableArgs().isPresent()) {
      addError(ErrorLabel.GENERIC, node, "Table arguments for expressions not implemented yet.");
    }
    List<String> names = SqrlListUtil.popLast(node.getIdentifier().names);
    Optional<RelOptTable> table = resolveModifiableTable(node, names);

    List<SqlNode> selectList = new ArrayList<>();
    selectList.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
        node.getExpression(),
        new SqlIdentifier(node.getIdentifier().names.get(node.getIdentifier().names.size()-1), SqlParserPos.ZERO)));


    SqlSelect select = new SqlSelectBuilder()
        .setSelectList(selectList)
        .setFrom(new SqlIdentifier(tablePath, SqlParserPos.ZERO))
        .build();
    validateTable(node, select, node.getTableArgs(),
        true);

    isMaterializeTable.put(node, true);
    return null;
  }

  @Override
  public Void visit(SqrlSqlQuery node, Void context) {
    boolean materializeSelf = materializeSelfQuery(node);
    isMaterializeTable.put(node,materializeSelf);

    visit((SqrlAssignment) node, null);
    validateTable(node, node.getQuery(), node.getTableArgs(), materializeSelf);

    return null;
  }

  /**
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
  public void validateTable(SqrlAssignment statement, SqlNode query,
      Optional<SqrlTableFunctionDef> tableArgs, boolean materializeSelf) {
    SqlValidator validator = planner.createSqlValidator();

    if (statement.getIdentifier().names.size() > 1) {
      validateHasNestedSelf(query);
    }

    Pair<List<FunctionParameter>, SqlNode> p = transformArgs(query, materializeSelf, tableArgs.orElseGet(()->new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of())));
    query = p.getRight();
    this.parameters.putAll(statement, p.getLeft());

    preprocessSql.put(statement, query);
    SqlNode sqlNode;
    Result result;
    SqlNode validated;

    try {
      List<String> parent = List.of();
      if (statement.getIdentifier().names.size() > 1) {
        parent = getParentPath(statement);
        SqlUserDefinedTableFunction sqrlTable = planner.getTableFunction(parent);
        if (sqrlTable == null) {
          throw addError(ErrorLabel.GENERIC, statement.getIdentifier()
                  .getComponent(statement.getIdentifier().names.size()-1),
              "Could not find parent assignment table: %s",
              flattenNames(parent));
        }
      }

      SqrlToValidatorSql sqrlToSql = new SqrlToValidatorSql(planner, errorCollector, uniqueId);
      result = sqrlToSql.rewrite(query, parent, statement.getTableArgs().orElseGet(()->
          new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of())));
      this.isA.putAll(sqrlToSql.getIsA());

      sqlNode = result.getSqlNode();

      framework.getSqrlOperatorTable().addPlanningFnc(result.getFncs());

      if (statement instanceof SqrlExpressionQuery) {
        SqlNode aggregate = ((SqrlSqlValidator) validator).getAggregate((SqlSelect) sqlNode);
        if (aggregate != null) {
          throw addError(ErrorLabel.GENERIC, aggregate,
              "Aggregate functions not yet allowed");
        }
      }

      validated = validator.validate(sqlNode);
    } catch (CalciteContextException e) {
      throw addError(ErrorLabel.GENERIC, e);
    } catch (CollectedException e) {
      return;
    } catch (Exception e) {
      e.printStackTrace();
      addError(e, ErrorLabel.GENERIC, statement, e.getMessage());
      return;
    }

    //we have enough to plan
    RelDataType type = validator.getValidatedNodeType(validated);
    fieldNames.put(statement, type.getFieldNames());
    try {
//      RelNode relNode = planner.plan(Dialect.CALCITE, validated);
//      type = relNode.getRowType();
//      return Pair.of(result, type);
    } catch (Exception e) {
      errorCollector.exception(ErrorLabel.GENERIC, e.getMessage());
    }
  }

  private void validateHasNestedSelf(SqlNode query) {
    if (query.getKind() == SqlKind.UNION) {
      throw addError(ErrorLabel.GENERIC, query, "Nested unions not yet supported");
    } else if (query.getKind() != SqlKind.SELECT) {
      throw addError(ErrorLabel.GENERIC, query,
          "Unknown nested query type. Must be SELECT or JOIN");
    }
    SqlSelect select = (SqlSelect) query;
    SqlNode lhs = select.getFrom();
    lhs = stripAs(lhs);
    while (lhs instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) lhs;
      lhs = join.getLeft();
      lhs = stripAs(lhs);
    }
    if (!(lhs instanceof SqlIdentifier)) {
      throw addError(ErrorLabel.GENERIC, lhs, "Must be a table reference that starts with '@'");
    }
    SqlIdentifier identifier = (SqlIdentifier) lhs;
    if (!identifier.names.get(0).equals(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      throw addError(ErrorLabel.GENERIC, lhs, "Table must start with with '@'");
    }
  }

  public static List<String> getParentPath(SqrlAssignment statement) {
    if (statement instanceof SqrlExpressionQuery) {
      if (statement.getIdentifier().names.size() > 2) {
        return SqrlListUtil.popLast(SqrlListUtil.popLast(statement.getIdentifier().names));
      } else {
        return SqrlListUtil.popLast(statement.getIdentifier().names);
      }
    } else {
      return SqrlListUtil.popLast(statement.getIdentifier().names);
    }
  }

  public static String flattenNames(List<String> nameList) {
    return String.join(".", nameList);
  }

  public static boolean isSelfTable(SqlNode sqlNode) {
    if (sqlNode instanceof SqlCall &&
        ((SqlCall) sqlNode).getOperandList().get(0) instanceof SqlIdentifier) {
      SqlIdentifier id = ((SqlIdentifier) ((SqlCall) sqlNode).getOperandList()
          .get(0));
      return id.names.size() == 1 &&
          id.names.get(0).equalsIgnoreCase("@");
    }
    return false;
  }

  private Pair<List<FunctionParameter>, SqlNode> transformArgs(SqlNode query, boolean materializeSelf, SqrlTableFunctionDef sqrlTableFunctionDef) {
    List<FunctionParameter> parameterList = toParams(sqrlTableFunctionDef.getParameters(), planner.createSqlValidator());

    SqlNode node = SqlNodeVisitor.accept(new SqlRelationVisitor<>() {

      @Override
      public SqlNode visitQuerySpecification(SqlSelect node, Object context) {
        return new SqlSelectBuilder(node)
            .setFrom(SqlNodeVisitor.accept(this, node.getFrom(), null))
            .rewriteExpressions(rewriteVariables(parameterList, materializeSelf))
            .build();
      }

      @Override
      public SqlNode visitAliasedRelation(SqlCall node, Object context) {
        return node.getOperator().createCall(node.getParserPosition(),
            SqlNodeVisitor.accept(this, node.getOperandList().get(0), null),
            node.getOperandList().get(1));
      }

      @Override
      public SqlNode visitTable(SqlIdentifier node, Object context) {
        return node;
      }

      @Override
      public SqlNode visitJoin(SqlJoin node, Object context) {
        return new SqlJoinBuilder(node)
            .setLeft(SqlNodeVisitor.accept(this, node.getLeft(), null))
            .setRight(SqlNodeVisitor.accept(this, node.getRight(), null))
            .rewriteExpressions(rewriteVariables(parameterList, materializeSelf))
            .build();
      }

      @Override
      public SqlNode visitSetOperation(SqlCall node, Object context) {
        return node.getOperator().createCall(node.getParserPosition(),
            node.getOperandList().stream()
                .map(o->SqlNodeVisitor.accept(this, o, context))
                .collect(Collectors.toList()));
      }
    }, query, null);

    return Pair.of(parameterList, node);

  }

  public SqlShuttle rewriteVariables(List<FunctionParameter> parameterList, boolean materializeSelf) {
    return new SqlShuttle(){
      @Override
      public SqlNode visit(SqlIdentifier id) {
        if (isSelfField(id.names) && !materializeSelf) {
          //Add to param list if not there
          String name = id.names.get(1);
          for (FunctionParameter p : parameterList) {
            SqrlFunctionParameter s = (SqrlFunctionParameter) p;
            if (s.isInternal() && s.getName().equalsIgnoreCase(name)) {
              //already exists, return dynamic param of index
              if (paramMapping.get(p) != null) {
                SqlDynamicParam dynamicParam1 = paramMapping.get(p);
                dynamicParam.put(id, dynamicParam1);
                return dynamicParam1;
              } else {
                throw new RuntimeException("unknown param");
              }
            }
          }

          //todo: Type?
          RelDataType anyType = planner.getTypeFactory().createSqlType(SqlTypeName.ANY);
          SqrlFunctionParameter functionParameter = new SqrlFunctionParameter(name,
              Optional.empty(), SqlDataTypeSpecBuilder
                .create(anyType), parameterList.size(), anyType,
              true);
          parameterList.add(functionParameter);
          SqlDynamicParam param = new SqlDynamicParam(functionParameter.getOrdinal(), id.getParserPosition());
          dynamicParam.put(id, param);
          paramMapping.put(functionParameter, param);

          return param;
        } else if (isVariable(id.names)) {
          if (id.names.size() > 1) {
            addError(ErrorLabel.GENERIC, id, "Nested variables not yet implemented");
            return id;
          }

          List<FunctionParameter> defs = parameterList.stream()
              .filter(f->f.getName().equalsIgnoreCase(id.getSimple()))
              .collect(Collectors.toList());

          if (defs.size() > 1) {
            throw addError(ErrorLabel.GENERIC, id, "Too many matching table arguments");
          }

          if (defs.size() != 1) {
            addError(ErrorLabel.GENERIC, id, "Could not find matching table arguments");
            return new SqlDynamicParam(0, id.getParserPosition());
          }

          FunctionParameter param = defs.get(0);


          if (paramMapping.get(param) != null) {
            SqlDynamicParam dynamicParam1 = paramMapping.get(param);
            dynamicParam.put(id, dynamicParam1);
            return dynamicParam1;
          }

          int index = param.getOrdinal();

          SqlDynamicParam p = new SqlDynamicParam(index, id.getParserPosition());

          dynamicParam.put(id, p);
          paramMapping.put(param, p);

          return p;
        }

        return super.visit(id);
      }




    };
  }
  public static boolean isVariable(List<String> names) {
    return names.get(0).startsWith("@") && names.get(0).length() > 1;
  }
  public static boolean isSelfField(List<String> names) {
    return names.get(0).equalsIgnoreCase("@") && names.size() > 1;
  }

  /**
   * if JOIN, it should be nested. if FROM, can be anywhere
   *
   * Append an alias.* to the end so we can use the same logic to determine
   * what it points to.
   *
   */
  @Override
  public Void visit(SqrlJoinQuery node, Void context) {
    visit((SqrlAssignment) node, null);
    boolean materializeSelf = node.getQuery().getFetch() != null;
    isMaterializeTable.put(node, materializeSelf);
    checkAssignable(node);
    List<String> path = SqrlListUtil.popLast(node.getIdentifier().names);
    if (path.isEmpty()) {
      throw addError(ErrorLabel.GENERIC, node.getIdentifier(),
          "Cannot assign join declaration on root");
    }

    /**
     * We want a SELECT lastAlias.* FROM ~ so query has proper number of fields.
     */
    Optional<String> lastAlias = extractLastAlias(node.getQuery().getFrom());
    if (lastAlias.isEmpty()) {
      throw addError(ErrorLabel.GENERIC, node.getQuery(), "Not a valid join declaration. "
          + "Could not derive table/alias of last join path item.");
    }

    SqlSelect select = new SqlSelectBuilder(node.getQuery())
        .setSelectList(List.of(new SqlIdentifier(List.of(lastAlias.get(), ""), SqlParserPos.ZERO)))
        .build();

    preprocessSql.put(node, select);
    validateTable(node, select, node.getTableArgs(), materializeSelf);
    return null;
  }

  private Optional<String> extractLastAlias(SqlNode from) {
    return from.accept(new SqlBasicVisitor<>(){
      @Override
      public Optional<String> visit(SqlLiteral literal) {
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlCall call) {
        if (call.getKind() == SqlKind.JOIN) {
          return ((SqlJoin) call).getRight().accept(this);
        } else if (call.getKind() == SqlKind.AS) {
          return call.getOperandList().get(1).accept(this);
        }

        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlNodeList nodeList) {
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlIdentifier id) {
        if (id.isSimple()) {
          return Optional.of(id.getSimple());
        }
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlDataTypeSpec type) {
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlDynamicParam param) {
        return Optional.empty();
      }

      @Override
      public Optional<String> visit(SqlIntervalQualifier intervalQualifier) {
        return Optional.empty();
      }
    });
  }

  private void checkAssignable(SqrlAssignment node) {
    List<String> path = SqrlListUtil.popLast(node.getIdentifier().names);
    if (path.isEmpty()) {
      return;
    }

    SqlUserDefinedTableFunction tableFunction = planner.getTableFunction(path);
    if (tableFunction == null) {
      addError(ErrorLabel.GENERIC, node, "Cannot column or query to table");
      return;
    }
  }

  @Override
  public Void visit(SqrlFromQuery node, Void context) {
    isMaterializeTable.put(node, false);

    visit((SqrlAssignment) node, null);
    if (node.getIdentifier().names.size() > 1) {
      addError(ErrorLabel.GENERIC, node.getIdentifier(), "FROM clause cannot be nested. Use JOIN instead.");
    }

    validateTable(node, node.getQuery(), node.getTableArgs(), false);

    return null;
  }



  /**
   * Validate as query, cannot be nested (probably can be but disallow). No joins.
   *
   * Must have Order by statement
   */
  @Override
  public Void visit(SqrlDistinctQuery node, Void context) {
    isMaterializeTable.put(node, true);
    if (node.getSelect().getOrderList() == null) {
      addError(ErrorLabel.GENERIC, node, "Order by statement must be specified");
    }

    if (node.getIdentifier().names.size() > 1) {
      addError(ErrorLabel.GENERIC, node, "Order by cannot be nested");
      return null;
    }

    preprocessSql.put(node, node.getSelect());

    validateTable(node, node.getSelect(), Optional.empty(), false);

    return null;
  }

  @Override
  public Void visit(SqrlAssignTimestamp node, Void context) {
    if (node.getIdentifier().isStar()) {
      throw addError(ErrorLabel.GENERIC, node.getIdentifier(), "Cannot assign timestamp to multiple import items");
    }
    if (node.getIdentifier().names.size() > 1) {
      throw addError(ErrorLabel.GENERIC, node.getIdentifier().getComponent(1), "Cannot assign timestamp to nested item");
    }

    Optional<RelOptTable> table = resolveModifiableTable(node, node.getAlias().orElse(node.getIdentifier()).names);

    Optional<RexNode> rexNode = table.flatMap(t-> {
        try {
          return Optional.of(planner.planExpression(node.getTimestamp(), t.getRowType()));
        } catch (Exception e) {
          addError(ErrorLabel.GENERIC, node.getTimestamp(), e.getMessage());
          return Optional.empty();
        }
      }
    );

    rexNode
        .filter(r->!(r instanceof RexInputRef))
        .ifPresent(r->{
          if (node.getTimestampAlias().isEmpty()) {
            addError(ErrorLabel.GENERIC, node.getTimestamp(), "Alias must be specified for expression timestamps");
          }
        });

    return null;
  }

  private Optional<RelOptTable> resolveTable(SqlNode node, List<String> names) {
    Optional<RelOptTable> table = Optional.ofNullable(framework.getCatalogReader().getTableFromPath(names));
    if (table.isEmpty()) {
      throw addError(ErrorLabel.GENERIC, node, "Could not find table: %s", flattenNames(names));
    }

    table.ifPresent((t)->this.tableMap.put(node, t));

    return table;
  }

  private Optional<RelOptTable> resolveModifiableTable(SqlNode node, List<String> names) {
    Optional<RelOptTable> table = Optional.ofNullable(framework.getCatalogReader().getTableFromPath(names));
    if (table.isEmpty()) {
      addError(ErrorLabel.GENERIC, node, "Could not find table: %s", flattenNames(names));
    }

    table.ifPresent((t)->this.tableMap.put(node, t));

    table.ifPresent((t) -> {
      ModifiableTable modTable = t.unwrap(ModifiableTable.class);
      if (modTable == null) {
        addError(ErrorLabel.GENERIC, node, "Table cannot have a column added: %s", flattenNames(names));
      } else if (modTable.isLocked()) {
        addError(ErrorCode.TABLE_LOCKED, node, "Cannot add column to locked table: %s", flattenNames(names));
      }
    });

    return table;
  }

  private static List<FunctionParameter> toParams(List<SqrlTableParamDef> params,
      SqlValidator validator) {
    List<FunctionParameter> parameters = params.stream()
        .map(p->new SqrlFunctionParameter(p.getName().getSimple(), p.getDefaultValue(),
            p.getType(), p.getIndex(), p.getType().deriveType(validator),p.isInternal()))
        .collect(Collectors.toList());
    return parameters;
  }

  public RuntimeException addError(ErrorLabel errorCode, CalciteContextException e) {
    RuntimeException exception = CheckUtil.createAstException(Optional.of(e), errorCode,
        ()->new SqlParserPos(e.getPosLine(), e.getPosColumn(), e.getEndPosLine(), e.getEndPosColumn()),
        e::getMessage);
    return errorCollector.handle(exception);
  }

  public static RuntimeException addError(ErrorCollector errorCollector, ErrorLabel errorCode, SqlNode node,
      String message, Object... format) {
    RuntimeException exception = CheckUtil.createAstException(errorCode, node, format == null ? message : String.format(message, format));
    return errorCollector.handle(exception);
  }

  public RuntimeException addError(Throwable cause, ErrorLabel errorCode, SqlNode node,
      String message, Object... format) {
    RuntimeException exception = CheckUtil.createAstException(Optional.of(cause), errorCode,
        node::getParserPosition, ()-> format == null || message == null ? message : String.format(message, format));
    return errorCollector.handle(exception);
  }

  private RuntimeException addError(ErrorLabel errorCode, SqlNode node,
      String message, Object... format) {
    return addError(errorCollector, errorCode, node, message, format);
  }

  private void addWarn(ErrorLabel errorCode, SqlNode node,
      String message, Object... format) {
    //todo: warn
    addError(errorCode, node, message, format);
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
