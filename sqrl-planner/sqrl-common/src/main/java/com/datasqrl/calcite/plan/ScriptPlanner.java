package com.datasqrl.calcite.plan;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.SqrlToSql;
import com.datasqrl.calcite.SqrlToSql.Result;
import com.datasqrl.calcite.NormalizeTablePath;
import com.datasqrl.calcite.TimestampAssignableTable;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.calcite.sqrl.PathToSql;
import com.datasqrl.calcite.visitor.SqlNodeVisitor;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.rel.LogicalStream;
import com.datasqrl.plan.validate.ScriptValidator;
import com.datasqrl.plan.validate.ScriptValidator.QualifiedExport;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.util.SqlNameUtil;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlAssignTimestamp;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlJoinQuery;
import org.apache.calcite.sql.SqrlStreamQuery;
import org.apache.calcite.sql.StatementVisitor;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

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

  @Override
  public Void visit(SqrlExportDefinition node, Void context) {
    QualifiedExport export = validator.getExportOps().get(node);
    ModifiableTable table = planner.getCatalogReader().getTableFromPath(export.getTable())
        .unwrap(ModifiableTable.class);

    ResolvedExport resolvedExport = exportTable(table, export.getSink(), planner.getRelBuilder());
    framework.getSchema().add(resolvedExport);

    return null;
  }

  @Override
  public Void visit(SqrlAssignTimestamp query, Void context) {
    List<String> tableName = query.getAlias().orElse(query.getIdentifier()).names;
    NamePath names = nameUtil.toNamePath(tableName);
    RelOptTable table = planner.getCatalogReader().getTableFromPath(names);
    RexNode node = planner.planExpression(query.getTimestamp(), table.getRowType());

    int timestampIndex;
    if (!(node instanceof RexInputRef) && query.getTimestampAlias().isEmpty()) {
      timestampIndex = addColumn(node, ReservedName.SYSTEM_TIMESTAMP.getCanonical(), table);
    } else if (query.getTimestampAlias().isPresent()) {
      //otherwise, add new column
      timestampIndex = addColumn(node, query.getTimestampAlias().get().getSimple(), table);
    } else {
      timestampIndex = ((RexInputRef) node).getIndex();
    }

    TimestampAssignableTable timestampAssignableTable = table.unwrap(TimestampAssignableTable.class);
    timestampAssignableTable.assignTimestamp(timestampIndex);

    return null;
  }

  @Override
  public Void visit(SqrlAssignment assignment, Void context) {
    SqlNode node = validator.getPreprocessSql().get(assignment);
    boolean materializeSelf = validator.getIsMaterializeTable().get(assignment);
    NamePath parentPath = getParentPath(assignment);
    NormalizeTablePath normalizeTablePath = new NormalizeTablePath(planner.getCatalogReader(),
        validator.getParamMapping(), new SqlNameUtil(planner.getFramework().getNameCanonicalizer()));
    SqrlToSql sqrlToSql = new SqrlToSql(planner.getCatalogReader(), planner.getOperatorTable(),
        normalizeTablePath, validator.getParameters().get(assignment), framework.getUniquePkId(), nameUtil);
    Result result = sqrlToSql.rewrite(node, materializeSelf, parentPath);

    RelNode relNode = planner.plan(Dialect.CALCITE, result.getSqlNode());
    RelNode expanded = planner.expandMacros(relNode);

    List<Function> isA = validator.getIsA().get(node);

    if (assignment instanceof SqrlJoinQuery) {
      List<SqrlTableMacro> isASqrl = isA.stream()
          .map(f->((SqrlTableMacro)f))
          .collect(Collectors.toList());
      NamePath path = nameUtil.toNamePath(assignment.getIdentifier().names);

      NamePath toTable = isASqrl.get(isASqrl.size()-1).getAbsolutePath();
      Supplier<RelNode> nodeSupplier = ()->expanded;

      Relationship rel = new Relationship(path.getLast(),
          path, toTable, Relationship.JoinType.JOIN, Multiplicity.MANY,
          result.getParams(), nodeSupplier);
      planner.getSchema().addRelationship(rel);
    } else {
      List<String> path = assignment.getIdentifier().names;
      RelNode rel = assignment instanceof SqrlStreamQuery
          ? LogicalStream.create(expanded, ((SqrlStreamQuery)assignment).getType())
          : expanded;

      Optional<Supplier<RelNode>> nodeSupplier = result.getParams().isEmpty()
          ? Optional.empty()
          : Optional.of(()->rel);

      tableFactory.createTable(path, rel, null,
          assignment.getHints(), result.getParams(), isA,
          materializeSelf, nodeSupplier, errors);
    }

    return null;
  }

  public NamePath getParentPath(SqrlAssignment statement) {
    NamePath path = nameUtil.toNamePath(statement.getIdentifier().names);
    if (statement instanceof SqrlExpressionQuery) {
      if (statement.getIdentifier().names.size() > 2) {
        return path.popLast().popLast();
      } else {
        return path.popLast();
      }
    } else {
      return path.popLast();
    }
  }
  @Override
  public Void visit(SqrlExpressionQuery node, Void context) {
    NamePath path = nameUtil.toNamePath(node.getIdentifier().names).popLast();
    RelOptTable table = planner.getCatalogReader().getTableFromPath(path);
    RexNode rexNode = planner.planExpression(node.getExpression(), table.getRowType());
    addColumn(rexNode, Util.last(node.getIdentifier().names), table);
    return null;
  }

  private int addColumn(RexNode node, String cName, RelOptTable table) {
    if (table.unwrap(ModifiableTable.class) != null) {
      ModifiableTable table1 = (ModifiableTable) table.unwrap(Table.class);
      return table1.addColumn(cName, node, framework.getTypeFactory());
    } else {
      throw new RuntimeException();
    }
  }

  public static ResolvedExport exportTable(ModifiableTable table, TableSink sink, RelBuilder relBuilder) {
    RelNode export = relBuilder.scan(table.getNameId())
        .build();
    return new ResolvedExport(table.getNameId(), export, sink);
  }
}
