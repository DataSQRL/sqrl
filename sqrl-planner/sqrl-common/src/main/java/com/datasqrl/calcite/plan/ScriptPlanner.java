package com.datasqrl.calcite.plan;

import static com.datasqrl.plan.validate.ScriptValidator.getParentPath;
import static com.datasqrl.plan.validate.ScriptValidator.isSelfField;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.SqrlToSql;
import com.datasqrl.calcite.SqrlToSql.Result;
import com.datasqrl.calcite.TimestampAssignableTable;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.rel.LogicalStream;
import com.datasqrl.plan.validate.ScriptValidator;
import com.datasqrl.plan.validate.ScriptValidator.QualifiedExport;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.util.SqlNameUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlAssignTimestamp;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.calcite.sql.SqrlFromQuery;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlStreamQuery;
import org.apache.calcite.sql.StatementVisitor;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class ScriptPlanner implements StatementVisitor<Void, Void> {

  private final QueryPlanner planner;
  private final ScriptValidator validator;
  private final SqrlTableFactory tableFactory;
  private final SqrlFramework framework;
  private final SqlNameUtil nameUtil;
  private final ErrorCollector errors;

  public Void plan(SqlNode query) {
    return SqlNodeVisitor.accept(this, query, null);
  }

  @Override
  public Void visit(SqrlImportDefinition node, Void context) {
    validator.getImportOps().get(node)
        .forEach(i->i.getObject().apply(i.getAlias(), framework, errors));
    return null;
  }

  public static ResolvedExport exportTable(ModifiableTable table, TableSink sink, RelBuilder relBuilder,
      boolean subscription) {
    relBuilder.scan(table.getNameId());
    List<RexNode> selects = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    table.getRowType().getFieldList().stream().forEach(c -> {
      selects.add(relBuilder.field(c.getName()));
      fieldNames.add(c.getName());
    });
    relBuilder.project(selects, fieldNames);
    return new ResolvedExport(table.getNameId(), relBuilder.build(), sink);
  }

  @Override
  public Void visit(SqrlExportDefinition node, Void context) {
    QualifiedExport export = validator.getExportOps().get(node);
    ModifiableTable table = planner.getCatalogReader().getTableFromPath(export.getTable())
        .unwrap(ModifiableTable.class);

    ResolvedExport resolvedExport = exportTable(table, export.getSink(),
        planner.getRelBuilder(), false);

    framework.getSchema().add(resolvedExport);

    return null;
  }

  @Override
  public Void visit(SqrlAssignTimestamp query, Void context) {
    List<String> tableName = query.getAlias().orElse(query.getIdentifier()).names;
    RelOptTable table = planner.getCatalogReader().getTableFromPath(tableName);

    RexNode node = planner.planExpression(query.getTimestamp(), table.getRowType());
    TimestampAssignableTable timestampAssignableTable = table.unwrap(TimestampAssignableTable.class);

    int timestampIndex;
    if (!(node instanceof RexInputRef) && query.getTimestampAlias().isEmpty()) {
      timestampIndex = addColumn(node, ReservedName.SYSTEM_TIMESTAMP.getCanonical(), table);
    } else if (query.getTimestampAlias().isPresent()) {
      //otherwise, add new column
      timestampIndex = addColumn(node, query.getTimestampAlias().get().getSimple(),
          planner.getCatalogReader().getTableFromPath(tableName));
    } else {
      timestampIndex = ((RexInputRef) node).getIndex();
    }
    timestampAssignableTable.assignTimestamp(timestampIndex);

    return null;
  }

  @Override
  public Void visit(SqrlAssignment assignment, Void context) {
    SqlNode node = validator.getPreprocessSql().get(assignment);
    boolean materializeSelf = validator.getIsMaterializeTable().get(assignment);
    List<String> parentPath = getParentPath(assignment);
    Result result = new SqrlToSql(planner.getCatalogReader(), nameUtil,
        planner.getOperatorTable(), validator.getDynamicParam(), framework.getUniquePkId())
        .rewrite(node, materializeSelf, parentPath);

    //Expanding table functions may have added additional parameters that we need to remove.
    //These can only be discovered either during sqrl to sql rewriting or directly after
    //TODO: move this logic into sqrl to sql converter and have it return params
    List<FunctionParameter> parameters = validator.getParameters().get(assignment);
    Pair<List<FunctionParameter>, SqlNode> rewritten = extractSelfArgs(parameters,
        materializeSelf, result.getSqlNode());
    parameters = rewritten.getLeft();

    RelNode relNode = planner.plan(Dialect.CALCITE, rewritten.getRight());
    RelNode expanded = planner.expandMacros(relNode);

    final Optional<SqlNode> sql = !materializeSelf
        ? Optional.of(planner.relToSql(Dialect.CALCITE, expanded))
        : Optional.empty();
    if (assignment instanceof SqrlStreamQuery) {
      expanded = LogicalStream.create(expanded, ((SqrlStreamQuery)assignment).getType());
    }

    List<Function> isA = validator.getIsA().get(node);

    //Short path: if we're not materializing, create relationship
    if (!materializeSelf) {
      NamePath path = nameUtil.toNamePath(assignment.getIdentifier().names);
      List<SqrlTableMacro> isASqrl = isA.stream()
          .map(f->((SqrlTableMacro)f))
          .collect(Collectors.toList());

      Supplier<RelNode> nodeSupplier = ()->framework.getQueryPlanner().plan(Dialect.CALCITE, sql.get());
      //if nested, add as relationship
      if (assignment.getIdentifier().names.size() > 1) {
        NamePath fromTable = path.popLast();
        NamePath toTable = isASqrl.get(0).getPath();
        String fromSysTable = planner.getSchema().getPathToSysTableMap().get(fromTable);
        String toSysTable = planner.getSchema().getPathToSysTableMap().get(toTable);

        Relationship rel = new Relationship(path.getLast(),
            path, fromSysTable, toSysTable, Relationship.JoinType.JOIN, Multiplicity.MANY,
            parameters, nodeSupplier);
        planner.getSchema().addRelationship(rel);
      } else {
        //todo fix for FROM statements
        final RelNode finalRel = expanded;
        //todo: unclean way to find from query
        nodeSupplier = assignment instanceof SqrlFromQuery ? nodeSupplier : ()->finalRel;

        ErrorCollector statementErrors = errors.atFile(SqrlAstException.toLocation(assignment.getParserPosition()));
        tableFactory.createTable(path.toStringList(),
            expanded, null,
            assignment.getHints(), parameters, isA,
            materializeSelf, Optional.of(nodeSupplier), statementErrors);
      }
    } else {
      ErrorCollector statementErrors = errors.atFile(SqrlAstException.toLocation(assignment.getParserPosition()));
      List<String> path = assignment instanceof SqrlExpressionQuery ?
          SqrlListUtil.popLast(assignment.getIdentifier().names) :
          assignment.getIdentifier().names;

      tableFactory.createTable(path, expanded, null,
          assignment.getHints(), parameters, isA,
          materializeSelf, Optional.empty(), statementErrors);
    }

    return null;
  }

  @Override
  public Void visit(SqrlExpressionQuery node, Void context) {
    RelOptTable table = planner.getCatalogReader().getTableFromPath(SqrlListUtil.popLast(node.getIdentifier().names));
    RexNode rexNode = planner.planExpression(node.getExpression(), table.getRowType());
    addColumn(rexNode, Util.last(node.getIdentifier().names), table);
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

  private int addColumn(RexNode node, String cName, RelOptTable table) {
    if (table.unwrap(ModifiableTable.class) != null) {
      ModifiableTable table1 = (ModifiableTable) table.unwrap(Table.class);
      return table1.addColumn(cName, node, framework.getTypeFactory());
    } else {
      throw new RuntimeException();
    }
  }
}
