package com.datasqrl.plan;

import static org.apache.calcite.sql.SqlUtil.stripAs;

import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.schema.PathWalker;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlAliasCallBuilder;
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
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.util.CalciteUtil.RelDataTypeFieldBuilder;
import com.datasqrl.util.CheckUtil;
import com.datasqrl.util.SqlNameUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
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
import org.apache.calcite.sql.SqrlTableParamDef;
import org.apache.calcite.sql.StatementVisitor;
import org.apache.calcite.sql.SqrlStreamQuery;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.calcite.shaded.com.google.common.collect.ArrayListMultimap;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

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
    Optional<RelOptTable> table = resolveModifiableTable(node.getIdentifier(), node.getIdentifier().names);
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
    visit((SqrlAssignment) node, null);
    if (node.getTableArgs().isPresent()) {
      addError(ErrorLabel.GENERIC, node, "Table arguments for expressions not implemented yet.");
    }
    List<String> names = SqrlListUtil.popLast(node.getIdentifier().names);
    Optional<RelOptTable> table = resolveModifiableTable(node, names);
    table.ifPresent((t) -> {
      ModifiableTable modTable = t.unwrap(ModifiableTable.class);
      if (modTable == null) {
        //TODO: isn't this already checked in resolveModifableTable?
        addError(ErrorLabel.GENERIC, node, "Table cannot have a column added: %s", flattenNames(names));
      } else if (modTable.isLocked()) {
        addError(ErrorCode.TABLE_LOCKED, node, "Cannot add column to locked table: %s", flattenNames(names));
      }

    });

    List<SqlNode> selectList = new ArrayList<>();
    selectList.add(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
        node.getExpression(),
        new SqlIdentifier(node.getIdentifier().names.get(node.getIdentifier().names.size()-1), SqlParserPos.ZERO)));

    //If on root table, use root table
    //If on nested, use @.table
    List<SqlNode> tablePath;
    if (node.getIdentifier().names.size() > 2) {
      tablePath = List.of(
          new SqlIdentifier(ReservedName.SELF_IDENTIFIER.getCanonical(), SqlParserPos.ZERO),
          new SqlIdentifier(node.getIdentifier().names.get(node.getIdentifier().names.size() - 2), SqlParserPos.ZERO));
    } else if (node.getIdentifier().names.size() == 2) {
      tablePath = List.of(new SqlIdentifier(node.getIdentifier().names.get(0), SqlParserPos.ZERO));
    } else {
      throw new RuntimeException();
    }

    selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
    SqlSelect select = new SqlSelectBuilder()
        .setSelectList(selectList)
        .setFrom(new SqrlCompoundIdentifier(SqlParserPos.ZERO, tablePath))
        .build();
    validateTable(node, select, node.getTableArgs(),
        true);

//    preprocessSql.put(node, select);
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

    //if query is nested, and the lhs is not already a '@', then add it
    if (statement.getIdentifier().names.size() > 1 && !(statement instanceof SqrlExpressionQuery)) {
      query = addNestedSelf(query);
    }
    Pair<List<FunctionParameter>, SqlNode> p = transformArgs(query, materializeSelf, tableArgs.orElseGet(()->new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of())));
    query = p.getRight();
    this.parameters.putAll(statement, p.getLeft());

    preprocessSql.put(statement, query);
    SqlNode sqlNode;
    Result result;
    SqlNode validated;

    try {
      SqrlToSql sqrlToSql = new SqrlToSql(planner);

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

      result = sqrlToSql.rewrite(query, parent, statement.getTableArgs().orElseGet(()->
          new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of())));
      sqlNode = result.getSqlNode();

      framework.getSqrlOperatorTable().addPlanningFnc(result.getFncs());

      SqlNode copy = sqlNode.accept(new SqlShuttle() {
        @Override
        public SqlNode visit(SqlIdentifier id) {
          return new SqlIdentifier(id.names, id.getParserPosition());
        }
      });
      validated = validator.validate(copy);

    } catch (CalciteContextException e) {
      e.printStackTrace();
      throw addError(ErrorLabel.GENERIC, e);
    } catch (CollectedException e) {
      return;
    } catch (Exception e) {
      e.printStackTrace();
      addError(ErrorLabel.GENERIC, statement, e.getMessage());
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

  private String flattenNames(List<String> nameList) {
    return String.join(".", nameList);
  }

  private List<String> getFieldNames(List<SqlNode> list) {
    List<String> nodes = new ArrayList<>();
    for (SqlNode node : list) {
      if (node instanceof SqlIdentifier) {
        String name = ((SqlIdentifier) node).names.get(((SqlIdentifier) node).names.size()-1);
        nodes.add(nameUtil.toName(name).getCanonical());
      } else if (node instanceof SqlCall && ((SqlCall)node).getKind() == SqlKind.AS) {
        String name = ((SqlIdentifier)((SqlCall) node).getOperandList().get(1)).names.get(0);
        nodes.add(nameUtil.toName(name).getCanonical());
      } else {
        throw new RuntimeException("Could not derive name: " + node);
      }
    }

    return nodes;
  }
  private SqlNode addNestedSelf(SqlNode query) {
    return SqlNodeVisitor.accept(new SqlRelationVisitor<SqlNode, Void>() {

      @Override
      public SqlNode visitQuerySpecification(SqlSelect node, Void context) {
        if (!isSelfTable(node.getFrom()) && !(node.getFrom() instanceof SqlJoin)
            && !isSelfPrefix(node.getFrom())) {
          SqlNode from = addSelfTableAsJoin(node.getFrom());
          return new SqlSelectBuilder(node)
              .setFrom(from)
              .build();
        }

        return new SqlSelectBuilder(node)
            .setFrom(SqlNodeVisitor.accept(this, node.getFrom(), null))
            .build();
      }

      private SqlNode addSelfTableAsJoin(SqlNode from) {
        return new SqlJoinBuilder()
            .setLeft(createSelfCall())
            .setRight(from)
            .build();
      }

      @Override
      public SqlNode visitAliasedRelation(SqlCall node, Void context) {
        return node;
      }

      @Override
      public SqlNode visitTable(SqrlCompoundIdentifier node, Void context) {
        return node;
      }

      @Override
      public SqlNode visitJoin(SqlJoin node, Void context) {
        if (!isSelfTable(node.getLeft()) && !(node.getLeft() instanceof SqlJoin)) {
          SqlNode join = addSelfTableAsJoin(node.getLeft());
          return new SqlJoinBuilder(node)
              .setLeft(join)
              .setRight(node.getRight())
              .build();
        }

        return new SqlJoinBuilder(node)
            .setLeft(SqlNodeVisitor.accept(this, node.getLeft(), null))
            .build();
      }

      @Override
      public SqlNode visitSetOperation(SqlCall node, Void context) {
        return node.getOperator().createCall(node.getParserPosition(),
            node.getOperandList().stream()
                .map(o->SqlNodeVisitor.accept(this, o, context))
                .collect(Collectors.toList()));
      }
    }, query, null);
  }

  private boolean isSelfPrefix(SqlNode sqlNode) {
    if (sqlNode instanceof SqrlCompoundIdentifier &&
        ((SqrlCompoundIdentifier)sqlNode).getList().get(0) instanceof SqlIdentifier) {
      return ((SqlIdentifier)((SqrlCompoundIdentifier)sqlNode).getList().get(0)).names.get(0).equals(
          ReservedName.SELF_IDENTIFIER.getCanonical());
    }

    return false;
  }

  public static boolean isSelfTable(SqlNode sqlNode) {
    if (sqlNode instanceof SqlCall &&
        ((SqlCall) sqlNode).getOperandList().get(0) instanceof SqrlCompoundIdentifier) {
      SqrlCompoundIdentifier id = ((SqrlCompoundIdentifier) ((SqlCall) sqlNode).getOperandList()
          .get(0));
      return id.getItems().size() == 1 && id.getItems().get(0) instanceof SqlIdentifier &&
          ((SqlIdentifier) (id.getItems().get(0))).names.size() == 1 &&
          ((SqlIdentifier) (id.getItems().get(0))).names.get(0).equalsIgnoreCase("@");
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
      public SqlNode visitTable(SqrlCompoundIdentifier node, Object context) {
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
  public static boolean isVariable(ImmutableList<String> names) {
    return names.get(0).startsWith("@") && names.get(0).length() > 1;
  }
  public static boolean isSelfField(ImmutableList<String> names) {
    return names.get(0).equalsIgnoreCase("@") && names.size() > 1;
  }

  @AllArgsConstructor
  public class SqrlToSql implements SqlRelationVisitor<Result, Context> {
    final QueryPlanner planner;

    public Result rewrite(SqlNode query, List<String> currentPath, SqrlTableFunctionDef tableArgs) {
      Context context = new Context(currentPath, new HashMap<>(), tableArgs, query);

      return SqlNodeVisitor.accept(this, query, context);
    }

    @Override
    public Result visitQuerySpecification(SqlSelect call, Context context) {
      // Copy query specification with new RelNode.
      Context newContext = new Context(context.currentPath, new HashMap<>(), context.tableFunctionDef, context.root);
      Result result = SqlNodeVisitor.accept(this, call.getFrom(), newContext);

      for (SqlNode node : call.getSelectList()) {
        node = stripAs(node);
        if (node instanceof SqlIdentifier) {
          SqlIdentifier ident = (SqlIdentifier) node;
          if (ident.isStar() && ident.names.size() == 1) {
            for (List<String> path : newContext.getAliasPathMap().values()) {
              SqlUserDefinedTableFunction sqrlTable = planner.getTableFunction(path);
              isA.put(context.root, sqrlTable.getFunction());
            }
          } else if (ident.isStar() && ident.names.size() == 2) {
            List<String> path = newContext.getAliasPath(ident.names.get(0));
            SqlUserDefinedTableFunction sqrlTable = planner.getTableFunction(path);
            isA.put(context.root, sqrlTable.getFunction());
          }
        }
      }

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

      if (item.getKind() == SqlKind.SELECT) {
        Context ctx = new Context(context.currentPath, new HashMap<>(), context.tableFunctionDef, context.root);

        return SqlNodeVisitor.accept(this, item, ctx);
      }

      String identifier = getIdentifier(item)
          .orElseThrow(()->new RuntimeException("Subqueries are not yet implemented"));

      boolean isAlias = context.hasAlias(identifier);
      boolean isSelf = identifier.equals(ReservedName.SELF_IDENTIFIER.getCanonical());
//      boolean isSchemaTable = Optional.of(identifier)
//          .map(i->planner.getTableFunction(List.of(i)) != null)
//          .orElse(false);
      SqlUserDefinedTableFunction tableFunction = planner.getTableFunction(List.of(identifier));

      TableFunction latestTable;
      if (tableFunction != null) {
        pathWalker.walk(identifier);
        latestTable = tableFunction.getFunction();
      } else if (isAlias) {
        if (!input.hasNext()) {
          throw addError(ErrorLabel.GENERIC, item, "Alias by itself.");
        }
        pathWalker.setPath(context.getAliasPath(identifier));
        //Walk the next one and push in table function
        item = input.next();


        Optional<String> nextIdentifier = getIdentifier(item);
        if (nextIdentifier.isEmpty()) {
          throw addError(ErrorLabel.GENERIC, item,
              "Table is not a valid identifier");
        }

        pathWalker.walk(nextIdentifier.get());
        //get table of current path (no args)
        SqlUserDefinedTableFunction table = planner.getTableFunction(pathWalker.getPath());

        if (table == null) {
          throw addError(ErrorLabel.GENERIC, item,"Could not find path: %s",
              flattenNames(pathWalker.getUserDefined()));
        }

        latestTable = table.getFunction();
      } else if (isSelf) {
        pathWalker.setPath(context.getCurrentPath());
        if (!input.hasNext()) {//treat self as a table
          SqlUserDefinedTableFunction table = planner.getTableFunction(context.getCurrentPath());
          if (table == null) {
            throw addError(ErrorLabel.GENERIC, item, "Could not find parent table: %s",
                flattenNames(context.getCurrentPath()));
          }
          latestTable = table.getFunction();
        } else { //treat self as a parameterized binding to the next function
          item = input.next();
          Optional<String> nextIdentifier = getIdentifier(item);
          if (nextIdentifier.isEmpty()) {
            throw addError(ErrorLabel.GENERIC, item,"Table is not a valid identifier");
          }
          pathWalker.walk(nextIdentifier.get());

          SqlUserDefinedTableFunction table = planner.getTableFunction(pathWalker.getPath());
          if (table == null) {
            throw addError(ErrorLabel.GENERIC, item,"Could not find table: %s",
                flattenNames(pathWalker.getUserDefined()));
          }
          latestTable = table.getFunction();
        }
      } else {
        throw addError(ErrorLabel.GENERIC, item,"Could not find table: %s",
            identifier);
      }

      while (input.hasNext()) {
        item = input.next();
        Optional<String> nextIdentifier = getIdentifier(item);
        if (nextIdentifier.isEmpty()) {
          throw addError(ErrorLabel.GENERIC, item,"Table is not a valid identifier");
        }
        pathWalker.walk(nextIdentifier.get());

        SqlUserDefinedTableFunction table = planner.getTableFunction(pathWalker.getPath());
        if (table == null) {
          throw addError(ErrorLabel.GENERIC, item, "Could not find table: %s",
              flattenNames(pathWalker.getUserDefined()));
        }
        latestTable = table.getFunction();
      }

      RelDataTypeFieldBuilder b = new RelDataTypeFieldBuilder(new FieldInfoBuilder(planner.getTypeFactory()));
      ((SqrlTableMacro) latestTable).getSqrlTable()
          .getFields().getColumns()
          .forEach(c->b.add(c.getName().getDisplay(), c.getType()));
      final RelDataType latestTable2 = b.build();

      String name = node.getDisplay() + "$validate$"+ uniqueId.incrementAndGet();
      SqlUserDefinedTableFunction fnc = new SqlPlannerTableFunction(name, latestTable2);

      List<SqlNode> args = node.getItems().stream()
          .filter(f-> f instanceof SqlCall)
          .map(f-> (SqlCall)f)
          .flatMap(f->f.getOperandList().stream())
          .collect(Collectors.toList());

      SqlCall call = fnc.createCall(SqlParserPos.ZERO, args);
      plannerFns.add(fnc);

      SqlCall call1 = SqlStdOperatorTable.COLLECTION_TABLE.createCall(SqlParserPos.ZERO, call);
      return new Result(call1, pathWalker.getAbsolutePath(), plannerFns);
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

      Context context1 = new Context(leftNode.currentPath,  context.aliasPathMap, context.tableFunctionDef, context.root);
      Result rightNode = SqlNodeVisitor.accept(this, call.getRight(), context1);

      SqlNode join = new SqlJoinBuilder(call)
          .rewriteExpressions(new WalkSubqueries(planner, context))
          .setLeft(leftNode.getSqlNode())
          .setRight(rightNode.getSqlNode())
          .lateral()
          .build();

      return new Result(join, rightNode.getCurrentPath(), plannerFns);
    }

    @Override
    public Result visitSetOperation(SqlCall node, Context context) {
      return new Result(
          node.getOperator().createCall(node.getParserPosition(),
              node.getOperandList().stream()
                  .map(o->SqlNodeVisitor.accept(this, o, context).getSqlNode())
                  .collect(Collectors.toList())),
          List.of(), plannerFns);
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
    SqlNode root;

    public void addAlias(String alias, List<String> currentPath) {
      aliasPathMap.put(alias, currentPath);
    }

    public boolean hasAlias(String alias) {
      return aliasPathMap.containsKey(alias);
    }

    public List<String> getAliasPath(String alias) {
      if (getAliasPathMap().get(alias) == null) {
        throw new RuntimeException("Could not find alias: " + alias);
      }
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
     * Add the implicit table join as sql since the fields are accessible
     */
    SqlJoinBuilder joinBuilder = new SqlJoinBuilder();
    joinBuilder.setLeft(createSelfCall());
    Pair<String, SqlNode> aliasedQuery = getOrSetJoinAlias(node.getQuery().getFrom());
    joinBuilder.setRight(aliasedQuery.getRight());

    SqlSelect select = new SqlSelectBuilder(node.getQuery())
        .setFrom(joinBuilder.build())
        .setSelectList(List.of(new SqlIdentifier(List.of(aliasedQuery.getLeft(), ""), SqlParserPos.ZERO)))
        .build();

    preprocessSql.put(node, select);
    validateTable(node, select, node.getTableArgs(), materializeSelf);

    return null;
  }

  private SqlNode createSelfCall() {
    return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
        new SqrlCompoundIdentifier(SqlParserPos.ZERO,
            List.of(new SqlIdentifier("@", SqlParserPos.ZERO))),
        new SqlIdentifier("@", SqlParserPos.ZERO));
  }

  private Pair<String, SqlNode> getOrSetJoinAlias(SqlNode from) {
    AtomicReference<String> reference = new AtomicReference<>();
    SqlNode newNode = from.accept(new SqlShuttle() {
      @Override
      public SqlNode visit(SqlCall call) {
        if (call.getKind() == SqlKind.JOIN) {
          SqlJoin join = (SqlJoin) call;
          SqlJoinBuilder builder = new SqlJoinBuilder(join);
          builder.setRight(join.getRight().accept(this));
          return builder.build();
        }
        if (call.getKind() == SqlKind.AS) {
          reference.set(((SqlIdentifier)call.getOperandList().get(1)).names.get(0));
          return call;
        }

        return super.visit(call);
      }

      @Override
      public SqlNode visit(SqlNodeList nodeList) {
        if (nodeList instanceof SqrlCompoundIdentifier) {
          reference.set("_a");
          return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, nodeList,
              new SqlIdentifier("_a", SqlParserPos.ZERO));
        }

        return super.visit(nodeList);
      }

    });

    return Pair.of(reference.get(), newNode);
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
//    if (!(tableFunction.getFunction() instanceof ModifiableSqrlTable)) {
//      addError(ErrorLabel.GENERIC, node, "Table not modifiable %s", path);
//    }

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
    if (node.getOrder().isEmpty()) {
      addError(ErrorLabel.GENERIC, node, "Order by statement must be specified");
    }

    if (node.getIdentifier().names.size() > 1) {
      addError(ErrorLabel.GENERIC, node, "Order by cannot be nested");
      return null;
    }

    SqlSelect select = new SqlSelectBuilder(node.getParserPosition())
        .setDistinctOnHint(List.of())
        .setSelectList(node.getOperands())
        .setFrom(node.getTable())
        .setOrder(node.getOrder())
        .build();

    preprocessSql.put(node, select);

    validateTable(node, select, Optional.empty(), false);

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

  private Optional<RelOptTable> resolveModifiableTable(SqlNode node, List<String> names) {
    Optional<RelOptTable> table = Optional.ofNullable(framework.getCatalogReader().getSqrlTable(names));
    if (table.isEmpty()) {
      addError(ErrorLabel.GENERIC, node, "Could not find table: %s", flattenNames(names));
    }

    table.ifPresent((t)->this.tableMap.put(node, t));

    table.ifPresent((t) -> {
      if (t.unwrap(ModifiableTable.class) == null) {
        addError(ErrorLabel.GENERIC, node, "Table cannot have a column added: %s", flattenNames(names));
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

  private List<RelHint> createHints(Optional<SqlNodeList> hints) {
    return hints.map(nodes -> nodes.getList().stream()
            .map(node -> RelHint.builder(((SqlHint) node).getName())
                .build())
            .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
  }

  public RuntimeException addError(ErrorLabel errorCode, CalciteContextException e) {
    RuntimeException exception = CheckUtil.createAstException(Optional.of(e), errorCode,
        ()->new SqlParserPos(e.getPosLine(), e.getPosColumn(), e.getEndPosLine(), e.getEndPosColumn()),
        e::getMessage);
    return errorCollector.handle(exception);
  }

  private RuntimeException addError(ErrorLabel errorCode, SqlNode node,
      String message, Object... format) {
    RuntimeException exception = CheckUtil.createAstException(errorCode, node, format == null ? message : String.format(message, format));
    return errorCollector.handle(exception);
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
