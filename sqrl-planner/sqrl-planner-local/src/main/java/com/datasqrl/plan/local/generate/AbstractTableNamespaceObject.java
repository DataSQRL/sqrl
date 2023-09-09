package com.datasqrl.plan.local.generate;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.schema.ScriptExecutor;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.plan.table.AbstractRelationalTable;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable.Child;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.SqlNameUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractTableNamespaceObject<T> implements TableNamespaceObject<T> {

  private final CalciteTableFactory tableFactory;
  private final Optional<SqrlTableFunctionDef> args;

  public AbstractTableNamespaceObject(CalciteTableFactory tableFactory, Optional<SqrlTableFunctionDef> args) {
    this.tableFactory = tableFactory;
    this.args = args;
  }


  protected boolean importSourceTable(Optional<String> objectName, TableSource table, SqrlFramework framework) {
    ScriptTableDefinition scriptTableDefinition = importTable(
        objectName.map(n->tableFactory.getCanonicalizer().name(n)),//todo fix
        table);

    registerScriptTable(scriptTableDefinition, framework);

    return true;
  }

  public ScriptTableDefinition importTable(
      Optional<Name> alias,
      TableSource source) {
    return tableFactory.importTable(source, alias);
  }

  public void registerScriptTable(ScriptTableDefinition tblDef, SqrlFramework framework) {
    framework.getSchema()
        .add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());
    //add to schema
    for (Map.Entry<SQRLTable, VirtualRelationalTable> entry : tblDef.getShredTableMap().entrySet()) {
      framework.getSchema().add(entry.getValue().getNameId(), entry.getValue());
      entry.getValue().setSqrlTable(entry.getKey());

      framework.getSchema().addSqrlTable(entry.getKey());
    }

    if (tblDef.getBaseTable() instanceof ProxyImportRelationalTable) {
      AbstractRelationalTable impTable = ((ProxyImportRelationalTable) tblDef.getBaseTable()).getBaseTable();
      framework.getSchema().add(impTable.getNameId(), impTable);
    }

    //Tables are available in planner, plan nodes
    for (Map.Entry<SQRLTable, VirtualRelationalTable> entry : tblDef.getShredTableMap().entrySet()) {
      SQRLTable table = entry.getKey();
      VirtualRelationalTable vt = entry.getValue();

      List<String> path = SqlNameUtil.toStringList(table.getPath());
      framework.getSchema().addTableMapping(path, entry.getValue().getNameId());

      VirtualRelationalTable childVt;
      VirtualRelationalTable parentVt;
      //create parent/child nodes
      if (!vt.isRoot() || path.size() > 1) {
        if (vt instanceof Child) {
          childVt = (Child) vt;
          parentVt = ((Child) vt).getParent();
        } else {
          parentVt = framework.getCatalogReader().getSqrlTable(SqrlListUtil.popLast(path))
              .unwrap(VirtualRelationalTable.class);
          childVt = framework.getCatalogReader().getSqrlTable(path)
              .unwrap(VirtualRelationalTable.class);

        }
        createNestedChild(framework, framework.getQueryPlanner().getRelBuilder(), childVt, parentVt, path);
        createParent(framework, framework.getQueryPlanner().getRelBuilder(), childVt, parentVt, path);

        List<String> parentPath = new ArrayList<>(path);
        parentPath.add("parent");
        List<String> child = path.subList(0, path.size()-1);

        framework.getSchema().addRelationship(parentPath, child);
      } else {
        RelNode rel = framework.getQueryPlanner().getRelBuilder()
            .scan(entry.getValue().getNameId())
            .build();

        SqlSelect node = (SqlSelect)framework.getQueryPlanner().relToSql(Dialect.CALCITE, rel);
        node.setSelectList(new SqlNodeList(List.of(SqlIdentifier.star(SqlParserPos.ZERO)), SqlParserPos.ZERO));

        SqlValidator validator = tableFactory.getFramework().getQueryPlanner().createSqlValidator();
        TableFunction function = ScriptExecutor.createFunction(validator,
            args.orElse(new SqrlTableFunctionDef(SqlParserPos.ZERO, List.of())),
            rel.getRowType(), node, entry.getValue().getNameId(),
            framework.getQueryPlanner().getCatalogReader());

        if (tblDef.getBaseTable() instanceof QueryRelationalTable) {
          QueryTableFunction queryTableFunction = new QueryTableFunction(
              Name.system(String.join(".", path)+"$"),
              function.getParameters(), table, (QueryRelationalTable) tblDef.getBaseTable());
          framework.getSchema().addPlannerTableFunction(queryTableFunction.getFunctionName().getCanonical(), queryTableFunction);
        }

        String name = framework.getSchema().getUniqueFunctionName(path);
        framework.getSchema().plus().add(name, function);
      }
    }
  }

  private void createNode(SqrlFramework framework, RelBuilder relBuilder, VirtualRelationalTable fromTable, VirtualRelationalTable toTable, List<String> path, boolean isChild) {
    List<RexNode> equality = new ArrayList<>();
    List<String> names = new ArrayList<>();
    RexBuilder rex = relBuilder.getRexBuilder();
    for (int i = 0; i < Math.min(fromTable.getNumPrimaryKeys(), toTable.getNumPrimaryKeys()); i++) {
      //create equality constraint of primary keys
      RelDataTypeField lhs = fromTable.getRowType().getFieldList().get(i);
      RelDataTypeField rhs = toTable.getRowType().getFieldList().get(i);

      RexDynamicParam param = new RexDynamicParam(lhs.getType(),
          lhs.getIndex());

      RexNode eq = rex.makeCall(SqlStdOperatorTable.EQUALS, param,
          rex.makeInputRef(rhs.getType(), rhs.getIndex()));

      names.add(lhs.getName());
      equality.add(eq);
    }

    String scanName = toTable.getNameId();
    RelNode relNode = relBuilder.scan(scanName)
        .filter(equality)
        .build();

    SqlNode node = framework.getQueryPlanner().relToSql(Dialect.CALCITE, relNode);
    System.out.println("X"+node);

    if (Math.min(fromTable.getNumPrimaryKeys(), toTable.getNumPrimaryKeys()) != 0) {
      List<SqrlTableParamDef> params = new ArrayList<>();
      for (int i = 0; i < Math.min(fromTable.getNumPrimaryKeys(), toTable.getNumPrimaryKeys()); i++) {
        //create equality constraint of primary keys
        RelDataTypeField lhs = fromTable.getRowType().getFieldList().get(i);
        params.add(new SqrlTableParamDef(SqlParserPos.ZERO,
            new SqlIdentifier("@"+lhs.getName(), SqlParserPos.ZERO),
            SqlDataTypeSpecBuilder.create(lhs.getType()),
            Optional.of(new SqlDynamicParam(lhs.getIndex(),SqlParserPos.ZERO)),
            params.size(),
            true));
      }

      SqlValidator validator = tableFactory.getFramework().getQueryPlanner().createSqlValidator();
      TableFunction function = ScriptExecutor.createFunction(validator,
          new SqrlTableFunctionDef(SqlParserPos.ZERO, params),
          relNode.getRowType(), node, scanName, framework.getQueryPlanner().getCatalogReader());

      String name = framework.getSchema().getUniqueFunctionName(path);
      framework.getSchema().plus().add(name, function);

      if (path.size() > 1) {
        fromTable.getSqrlTable().addRelationship(Name.system(path.get(path.size()-1)), toTable.getSqrlTable(),
            isChild ? Relationship.JoinType.CHILD : Relationship.JoinType.PARENT,
            isChild ? Multiplicity.MANY : Multiplicity.ONE);
      }
      //get sqrl table, add
    }
  }

  private void createNestedChild(SqrlFramework framework, RelBuilder relBuilder, VirtualRelationalTable vt,VirtualRelationalTable parent,List<String> path) {
    createNode(framework, relBuilder, parent, vt, path, true);
  }

  private void createParent(SqrlFramework framework, RelBuilder relBuilder, VirtualRelationalTable vt,VirtualRelationalTable parent, List<String> path) {
    List<String> p = new ArrayList<>(path);
    p.add("parent");
    createNode(framework, relBuilder, vt, parent, p, false);
  }
}
