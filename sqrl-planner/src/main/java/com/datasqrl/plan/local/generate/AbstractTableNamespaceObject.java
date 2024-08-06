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
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.plan.table.AbstractRelationalTable;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.ImportedRelationalTableImpl;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.PrimaryKey;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.TableConverter;
import com.datasqrl.plan.table.TableConverter.SourceTableDefinition;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.NestedRelationship;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
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
  private final ModuleLoader moduleLoader;

  public AbstractTableNamespaceObject(CalciteTableFactory tableFactory, NameCanonicalizer canonicalizer,
      ModuleLoader moduleLoader) {
    this.tableConverter = tableFactory.getTableConverter();
    this.tableFactory = tableFactory;
    this.canonicalizer = canonicalizer;
    this.moduleLoader = moduleLoader;
  }

  protected boolean importSourceTable(Optional<String> objectName, TableSource table,
      SqrlFramework framework, ErrorCollector errors) {
    ProxyImportRelationalTable importTable = importTable(table,
        objectName.map(canonicalizer::name).orElse(table.getName()), errors);
    registerScriptTable(importTable, framework, Optional.empty(), Optional.empty(), false);
    return true;
  }

  public ProxyImportRelationalTable importTable(TableSource tableSource, Name tableName,
      ErrorCollector errors) {
    // Convert the source schema to a universal table
    SourceTableDefinition tableDef = tableConverter.sourceToTable(
        tableSource.getTableSchema().get(),
        tableSource.getConfiguration(),
        tableName,
        moduleLoader,
        errors
    );

    // Create imported table and its proxy with unique IDs
    ImportedRelationalTableImpl importedTable = tableFactory.createImportedTable(tableDef.getDataType(), tableSource, tableName);
    ProxyImportRelationalTable proxyTable = tableFactory.createProxyTable(tableDef.getDataType(), NamePath.of(tableName),
        importedTable, tableDef.getTableType(), tableDef.getTimestampIndex(), tableDef.getPrimaryKey());

    // Generate the script tables based on the provided root table
    return proxyTable;
  }

  public void registerScriptTable(PhysicalRelationalTable table, SqrlFramework framework, Optional<List<FunctionParameter>> parameters,
      Optional<Supplier<RelNode>> relNodeSupplier, boolean isTest) {

    NamePath path =  table.getTablePath();

    framework.getSchema().add(table.getNameId(), table);
    framework.getSchema().addTableMapping(path, table.getNameId());
    if (table instanceof ProxyImportRelationalTable) {
      AbstractRelationalTable impTable = ((ProxyImportRelationalTable) table).getBaseTable();
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
          nodeSupplier, isTest);
      framework.getSchema().addTable(tbl);
    } else { //add parent-child relationships
      NamePath parentPath = path.popLast();
      String parentId = framework.getSchema().getPathToSysTableMap().get(parentPath);
      Preconditions.checkNotNull(parentId);
      PhysicalRelationalTable parentTable = (PhysicalRelationalTable) framework.getSchema().getTable(parentId, false)
          .getTable();;
      Preconditions.checkArgument(parentTable.getPrimaryKey().isDefined());
      Preconditions.checkArgument(table.getPrimaryKey().isDefined());

      Pair<List<FunctionParameter>, SqlNode> pkWrapper = createPkWrapper(parentTable, table);
      Multiplicity multiplicity = parentTable.getPrimaryKey().size()<table.getPrimaryKey().size()?
                                  Multiplicity.MANY : Multiplicity.ZERO_ONE;
      Relationship relationship = new Relationship(path.getLast(), path, path,
          JoinType.CHILD,
          multiplicity,
          pkWrapper.getLeft(),
          () -> framework.getQueryPlanner().plan(Dialect.CALCITE, pkWrapper.getRight()),
          isTest);
      framework.getSchema().addRelationship(relationship);

      Relationship rel = createParent(framework, path, parentTable, table, isTest);
      framework.getSchema().addRelationship(rel);
    }
    //Add nested relationships
    createNested(table.getRowType(), path, framework, isTest);
  }

  private void createNested(RelDataType type, NamePath path, SqrlFramework framework,
      boolean isTest) {
    type.getFieldList().stream().filter(f -> CalciteUtil.isNestedTable(f.getType()))
        .forEach(field -> createdNestedChild(field, path, framework, isTest));
  }

  private void createdNestedChild(RelDataTypeField field, NamePath path, SqrlFramework framework,
      boolean isTest) {
    path = path.concat(canonicalizer.name(field.getName()));
    RelDataType nestedType = CalciteUtil.getNestedTableType(field.getType()).get();
    Multiplicity multiplicity = Multiplicity.MANY;
    //TODO: We make the hard-coded assumption that the first field is the pk-field for now
    int[] pks = {0};
    if (!CalciteUtil.isArray(field.getType())) {
      if (field.getType().isNullable()) multiplicity = Multiplicity.ZERO_ONE;
      else multiplicity = Multiplicity.ONE;
      pks = new int[0];
    }
    Name name = path.getLast();
    SqrlFunctionParameter param = new SqrlFunctionParameter(
        name.getDisplay(),
        Optional.empty(),
        SqlDataTypeSpecBuilder.create(field.getType()),
        0,
        field.getType(),
        true, new CasedParameter(field.getName()));
    NestedRelationship relationship = new NestedRelationship(name, path, path,
        multiplicity, List.of(param), nestedType, pks, false);
    framework.getSchema().addRelationship(relationship);
    createNested(nestedType, path, framework, false);
  }

  public Relationship createParent(SqrlFramework framework, NamePath path, PhysicalRelationalTable parentTable,
      PhysicalRelationalTable childTable, boolean isTest) {
    Pair<List<FunctionParameter>, SqlNode> pkWrapper = createPkWrapper(childTable,
        parentTable);
    NamePath relPath = path.concat(ReservedName.PARENT);

    return new Relationship(ReservedName.PARENT,
        relPath, path.popLast(),
        JoinType.PARENT, Multiplicity.ONE, pkWrapper.getLeft(),
        () -> framework.getQueryPlanner().plan(Dialect.CALCITE, pkWrapper.getRight()),
        isTest);
  }

  public static Pair<List<FunctionParameter>, SqlNode> createPkWrapper(PhysicalRelationalTable fromTable,
      PhysicalRelationalTable toTable) {
    //Tables must have defined primary keys
    Preconditions.checkArgument(fromTable.getPrimaryKey().isDefined());
    Preconditions.checkArgument(toTable.getPrimaryKey().isDefined());
    //Parameters
    List<FunctionParameter> parameters = new ArrayList<>();
    List<SqlNode> conditions = new ArrayList<>();
    PrimaryKey toPk = toTable.getPrimaryKey(), fromPk = fromTable.getPrimaryKey();
    //This is for a parent-child relationship, meaning the first indexes are shared
    for (int i = 0; i < Math.min(toPk.size(), fromPk.size()); i++) {
      RelDataTypeField toField = toTable.getRowType().getFieldList().get(toPk.get(i));
      RelDataTypeField fromField = fromTable.getRowType().getFieldList().get(fromPk.get(i));

      SqrlFunctionParameter param = new SqrlFunctionParameter(
          toField.getName(),
          Optional.empty(),
          SqlDataTypeSpecBuilder.create(fromField.getType()),
          i,
          fromField.getType(),
          true, new CasedParameter(fromField.getName()));

      SqlDynamicParam dynamicParam = new SqlDynamicParam(i, SqlParserPos.ZERO);
      parameters.add(param);
      conditions.add(SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
          new SqlIdentifier(toField.getName(), SqlParserPos.ZERO),
          dynamicParam));
    }

    return Pair.of(parameters, new SqlSelectBuilder()
        .setFrom(new SqlIdentifier(toTable.getNameId(), SqlParserPos.ZERO))
        .setWhere(conditions)
        .build());
  }
}
