package com.datasqrl.plan.local.generate;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.function.SqrlFunctionParameter.CasedParameter;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.plan.table.*;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.schema.UniversalTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;

public abstract class AbstractTableNamespaceObject<T> implements TableNamespaceObject<T> {

  private final TableConverter tableConverter;
  private final CalciteTableFactory tableFactory;
  private final NameCanonicalizer canonicalizer;

  public AbstractTableNamespaceObject(CalciteTableFactory tableFactory, NameCanonicalizer canonicalizer) {
    this.tableConverter = tableFactory.getTableConverter();
    this.tableFactory = tableFactory;
    this.canonicalizer = canonicalizer;
  }

  protected boolean importSourceTable(Optional<String> objectName, TableSource table,
      SqrlFramework framework, ErrorCollector errors) {
    Map<NamePath, ScriptRelationalTable> tables = importTable(table,
        objectName.map(canonicalizer::name).orElse(table.getName()), errors);
    registerScriptTable(new ScriptTableDefinition(tables), framework, Optional.empty(), Optional.empty());
    return true;
  }

  public Map<NamePath, ScriptRelationalTable> importTable(TableSource tableSource, Name tableName,
      ErrorCollector errors) {
    // Convert the source schema to a universal table
    UniversalTable rootTable = tableConverter.sourceToTable(
        tableSource.getTableSchema().get(),
        tableSource.getConnectorSettings().isHasSourceTimestamp(),
        tableName,
        errors
    );

    // Convert the universal table to a Calcite relational data type
    RelDataType rootType = tableConverter.tableToDataType(rootTable);

    // Create imported table and its proxy with unique IDs
    ImportedRelationalTableImpl importedTable = tableFactory.createImportedTable(rootType, tableSource, rootTable.getName());
    ProxyImportRelationalTable proxyTable = tableFactory.createProxyTable(rootType, rootTable, importedTable);

    // Generate the script tables based on the provided root table
    return tableFactory.createScriptTables(rootTable, proxyTable);
  }

  public void registerScriptTable(ScriptTableDefinition tblDef, SqrlFramework framework, Optional<List<FunctionParameter>> parameters,
      Optional<Supplier<RelNode>> relNodeSupplier) {

    for(Entry<NamePath, ScriptRelationalTable> entry : tblDef.getShredTableMap().entrySet()) {
      NamePath path = entry.getKey();
      ScriptRelationalTable table = entry.getValue();

      framework.getSchema().add(table.getNameId(), table);
      framework.getSchema().addTableMapping(path, table.getNameId());
      if (tblDef.getBaseTable() instanceof ProxyImportRelationalTable) {
        AbstractRelationalTable impTable = ((ProxyImportRelationalTable) tblDef.getBaseTable()).getBaseTable();
        framework.getSchema().add(impTable.getNameId(), impTable);
      }

      if (path.size() == 1) {
        framework.getSchema().addTableMapping(path, table.getNameId());
        Supplier<RelNode> nodeSupplier = relNodeSupplier
            .orElse(() -> framework.getQueryPlanner().getRelBuilder().scan(table.getNameId()).build());
        RootSqrlTable tbl = new RootSqrlTable(
            path.getFirst(),
            table,
            parameters.orElse(List.of()),
            nodeSupplier);
        framework.getSchema().addTable(tbl);
      } else { //add nested
        NamePath parentPath = path.popLast();
        String parentId = framework.getSchema().getPathToSysTableMap().get(parentPath);
        Preconditions.checkNotNull(parentId);
        Table parentTable = framework.getSchema().getTable(parentId, false)
            .getTable();
        ScriptRelationalTable scriptParentTable = (ScriptRelationalTable) parentTable;

        Pair<List<FunctionParameter>, SqlNode> pkWrapper = createPkWrapper(scriptParentTable, table);
        Relationship relationship = new Relationship(path.getLast(), path, path,
            JoinType.CHILD,
            Multiplicity.MANY, //todo fix multiplicity
            pkWrapper.getLeft(),
            () -> framework.getQueryPlanner().plan(Dialect.CALCITE, pkWrapper.getRight()));
        framework.getSchema().addRelationship(relationship);

        Relationship rel = createParent(framework, path, scriptParentTable, table);
        framework.getSchema().addRelationship(rel);
      }
    }
  }

  public Relationship createParent(SqrlFramework framework, NamePath path, ScriptRelationalTable parentScriptTable,
      ScriptRelationalTable childScriptTable) {
    Pair<List<FunctionParameter>, SqlNode> pkWrapper = createPkWrapper(childScriptTable,
        parentScriptTable);
    NamePath relPath = path.concat(ReservedName.PARENT);

    return new Relationship(ReservedName.PARENT,
        relPath, path.popLast(),
        JoinType.PARENT, Multiplicity.ONE, pkWrapper.getLeft(),
        () -> framework.getQueryPlanner().plan(Dialect.CALCITE, pkWrapper.getRight()));
  }

  public static Pair<List<FunctionParameter>, SqlNode> createPkWrapper(ScriptRelationalTable fromTable,
      ScriptRelationalTable toTable) {
    //Parameters
    List<FunctionParameter> parameters = new ArrayList<>();
    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < Math.min(toTable.getNumPrimaryKeys(), fromTable.getNumPrimaryKeys()); i++) {
      RelDataTypeField field1 = toTable.getRowType().getFieldList().get(i);
      RelDataTypeField field = fromTable.getRowType().getFieldList().get(i);

      SqrlFunctionParameter param = new SqrlFunctionParameter(
          field1.getName(),
          Optional.empty(),
          SqlDataTypeSpecBuilder.create(field.getType()),
          i,
          field.getType(),
          true, new CasedParameter(field.getName()));

      SqlDynamicParam dynamicParam = new SqlDynamicParam(i, SqlParserPos.ZERO);
      parameters.add(param);
      conditions.add(SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
          new SqlIdentifier(field1.getName(), SqlParserPos.ZERO),
          dynamicParam));
    }

    return Pair.of(parameters, new SqlSelectBuilder()
        .setFrom(new SqlIdentifier(toTable.getNameId(), SqlParserPos.ZERO))
        .setWhere(conditions)
        .build());
  }
}
