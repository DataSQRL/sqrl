package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.CatalogReader;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.ModifiableSqrlTable;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlRelBuilder;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.TimestampAssignableTable;
import com.datasqrl.calcite.schema.op.LogicalAddColumnOp;
import com.datasqrl.calcite.schema.op.LogicalAssignTimestampOp;
import com.datasqrl.calcite.schema.op.LogicalCreateAliasOp;
import com.datasqrl.calcite.schema.op.LogicalCreateReferenceOp;
import com.datasqrl.calcite.schema.op.LogicalCreateStreamOp;
import com.datasqrl.calcite.schema.op.LogicalCreateTableOp;
import com.datasqrl.calcite.schema.op.LogicalExportOp;
import com.datasqrl.calcite.schema.op.LogicalImportOp;
import com.datasqrl.calcite.schema.op.LogicalOp;
import com.datasqrl.calcite.schema.op.LogicalOpVisitor;
import com.datasqrl.calcite.schema.op.LogicalSchemaModifyOps;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.SqlNameUtil;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.SqrlTableParamDef;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

@AllArgsConstructor
public class ScriptExecutor implements LogicalOpVisitor<Object, Object> {

  SqrlTableFactory tableFactory;
  SqrlFramework framework;
  SqlNameUtil nameUtil;
  ModuleLoader moduleLoader;
  ErrorCollector errors;

  public void apply(RelNode relNode) {
    if (relNode instanceof LogicalOp) {
      ((LogicalOp) relNode).accept(this, null);
      return;
    }

    throw new RuntimeException("Unknown op: " + relNode.getClass());
  }

  @Override
  public Object visit(LogicalSchemaModifyOps ops, Object context) {
    for (RelNode op : ops.getInputs()) {
      apply(op);
    }
    return null;
  }

  @Override
  public Object visit(LogicalImportOp relNode, Object context) {
    relNode.getImportList()
        .forEach(i->i.getObject().apply(i.getAlias(), framework, errors));

    return null;
  }

  @Override
  public Object visit(LogicalAssignTimestampOp relNode, Object context) {
    if (relNode.getInput() instanceof LogicalOp) {
      ((LogicalOp) relNode.getInput()).accept(this, context);
    }

    relNode.getTable().unwrap(TimestampAssignableTable.class)
        .assignTimestamp(relNode.getIndex());
    return null;
  }

  @Override
  public Object visit(LogicalExportOp relNode, Object context) {
    ResolvedExport export = new ResolvedExport(relNode.getTable().getQualifiedName().get(0),
        relNode.getInput(), relNode.getExport().getSink());
    framework.getSchema().add(export);

    return null;
  }

  @Override
  public Object visit(LogicalCreateReferenceOp relNode, Object context) {
    QueryPlanner planner = framework.getQueryPlanner();

    SqlNode node = planner.relToSql(Dialect.CALCITE, relNode.getInput());
    //todo: assure select * gets smushed
    System.out.println(node);

    RelOptTable relOptTable = planner.getCatalogReader().getSqrlTable(relNode.getTableReferences().get(0));
    TableFunction function = createFunction(planner.createSqlValidator(),
        relNode.getDef(), relOptTable.getRowType(),
        node, relOptTable.getQualifiedName().get(0), planner.getCatalogReader());

    String name = framework.getSchema().getUniqueFunctionName(relNode.getFromPath());
    framework.getSchema().plus().add(name, function);

    if (relNode.getFromPath().size() > 1) {
      SQRLTable table = framework.getQueryPlanner().getCatalogReader()
          .getSqrlTable(SqrlListUtil.popLast(relNode.getFromPath()))
          .unwrap(ModifiableSqrlTable.class).getSqrlTable();
      SQRLTable toTable = relOptTable.unwrap(ModifiableSqrlTable.class).getSqrlTable();

      table.addRelationship(nameUtil.toName(Util.last(relNode.getFromPath())), toTable, JoinType.CHILD, Multiplicity.MANY);
      this.framework.getSchema().addRelationship(
          relNode.getFromPath(), toTable.getPath().toStringList());
    }

    return null;
  }



  @Override
  public Object visit(LogicalCreateAliasOp relNode, Object context) {
    return null;
  }

  @Override
  public Object visit(LogicalCreateTableOp relNode, Object context) {
    createTable(relNode);
    return null;
  }

  @Override
  public Object visit(LogicalCreateStreamOp relNode, Object context) {
    createTable(relNode);
    return null;
  }

  @Override
  public Object visit(LogicalAddColumnOp relNode, Object context) {
    addColumn(relNode);
    return null;
  }

  private void addColumn(LogicalAddColumnOp op) {
    if (op.getToTable().unwrap(ModifiableSqrlTable.class) != null) {
      ModifiableSqrlTable table1 = (ModifiableSqrlTable) op.getToTable().unwrap(Table.class);
      String name = uniquifyColumnName(op.getName(), op.getToTable().getRowType().getFieldNames());
      table1.addColumn(nameUtil.toName(name).getCanonical(), op.getColumn(), framework.getTypeFactory());
      SQRLTable sqrlTable = table1.getSqrlTable();
      sqrlTable.addColumn(Name.system(op.getName()), nameUtil.toName(name),
          true, op.getColumn().getType());
    } else {
      throw new RuntimeException();
    }
  }

  public static String uniquifyColumnName(String name, List<String> names) {
    return name + "$" + SqrlRelBuilder.getNextVersion(names, name);
  }

  private void createTable(LogicalCreateTableOp op) {
    if (op.getArgs().getParameters().isEmpty() || !hasPublicArgs(op.getArgs().getParameters())) {
      tableFactory.createTable(op.getPath(), op.getInput(), op.getHints(), op.isSetFieldNames(),
          op.getOpHints(), op.getArgs());
    } else {
      SqlNode node = framework.getQueryPlanner().relToSql(Dialect.CALCITE, op.getInput());

      TableFunction function = ScriptExecutor.createFunction(
          this.framework.getQueryPlanner().createSqlValidator(),
          op.getArgs(),
          op.getInput().getRowType(), node, op.getPath().get(0) + "$" + 0,
          framework.getQueryPlanner().getCatalogReader());

      this.framework.getSchema().plus().add(op.getPath().get(0) + "$" + 0, function);
      SQRLTable sqrlTable = new SQRLTable(
          NamePath.of(op.getPath().toArray(String[]::new)), null, 0);
      for (RelDataTypeField name : op.getInput().getRowType().getFieldList()) {
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
}