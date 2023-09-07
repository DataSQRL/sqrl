package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.CatalogReader;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.ModifiableSqrlTable;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.TimestampAssignableTable;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.SqlNameUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.CalciteFixes;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.SqrlTableParamDef;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;
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
    NamePath path = nameUtil.toNamePath(relNode.getImportPath());

    SqrlModule module = moduleLoader.getModule(path.popLast()).orElseThrow(
        () -> errors.exception("Could not find module [%s] in: %s", path, moduleLoader));

    boolean loaded;
    if (path.getLast().equals(ReservedName.ALL)) {
      loaded = module.getNamespaceObjects().stream()
          .allMatch(obj -> obj.apply(Optional.empty(), framework, errors));
    } else {
      // Get the namespace object specified in the import statement
      NamespaceObject obj = module.getNamespaceObject(path.getLast()).orElseThrow(
          () -> new RuntimeException(String.format("Could not resolve import [%s]", path)));

      //Keep original casing
      String objectName = relNode.getAlias().orElse(path.getLast().getDisplay());

      // Add the namespace object to the current namespace
      loaded = obj.apply(Optional.of(objectName), framework, errors);
    }

    // Check if import loaded successfully
    if (!loaded || errors.hasErrors()) {
      throw new RuntimeException(String.format("Could not load import [%s]", path));
    }

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
    TableSink sink = LoaderUtil.loadSink(nameUtil.toNamePath(relNode.getSinkPath()), errors,
        moduleLoader);

    ResolvedExport export = new ResolvedExport(relNode.getTable().getQualifiedName().get(0), relNode.getInput(), sink);
    framework.getSchema().add(export);

    return null;
  }

  @Override
  public Object visit(LogicalCreateReference relNode, Object context) {
    QueryPlanner planner = framework.getQueryPlanner();

    SqlNode node = planner.relToSql(Dialect.CALCITE, relNode.getInput());
    //todo: assure select * gets smushed
    System.out.println(node);

    PreparingTable relOptTable = planner.getCatalogReader().getSqrlTable(relNode.getTableReferences().get(0));
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
  public Object visit(LogicalCreateAlias relNode, Object context) {
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
    RelOptTable table = op.getTable();
    if (table.unwrap(ModifiableSqrlTable.class) != null) {
      ModifiableSqrlTable table1 = (ModifiableSqrlTable) table.unwrap(Table.class);
      String name = uniquifyColumnName(op.getName(), table.getRowType().getFieldNames());
      table1.addColumn(nameUtil.toName(name).getCanonical(), op.getColumn(), framework.getTypeFactory());
      SQRLTable sqrlTable = table1.getSqrlTable();
      sqrlTable.addColumn(Name.system(op.getName()), nameUtil.toName(name),
          true, op.column.getType());
    } else {
      throw new RuntimeException();
    }
  }

  public static String uniquifyColumnName(String name, List<String> names) {
    names = names.stream()
        .map(n->n.toLowerCase())
        .collect(Collectors.toList());;
        name = name.toLowerCase();

    if (names.contains(name)) {
      return org.apache.calcite.sql.validate.SqlValidatorUtil.uniquify(
          name,
          new HashSet<>(names),
          //Renamed columns to names the user cannot address to prevent collisions
          (original, attempt, size) -> original + "$" + attempt);
    }

    return name;
  }

  private void createTable(LogicalCreateTableOp op) {
    tableFactory.createTable(op.path, op.input, op.hints, op.setFieldNames, op.getOpHints(), op.getArgs());
  }

  public static TableFunction createFunction(SqlValidator validator, SqrlTableFunctionDef def, RelDataType type,
      SqlNode node, String tableName, CatalogReader catalogReader) {
    SqrlTableFunction tableFunction = new SqrlTableFunction(toParams(def.getParameters(), validator),
        node, tableName, catalogReader);
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