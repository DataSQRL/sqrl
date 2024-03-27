package com.datasqrl.loaders;

import static com.datasqrl.function.FlinkUdfNsObject.getFunctionName;
import static com.datasqrl.io.tables.TableConfig.Base.PRIMARYKEY_KEY;
import static com.datasqrl.io.tables.TableConfig.Base.TIMESTAMP_COL_KEY;
import static com.datasqrl.io.tables.TableConfig.Base.TYPE_KEY;
import static com.datasqrl.io.tables.TableConfig.Base.WATERMARK_KEY;
import static com.datasqrl.io.tables.TableConfig.CONNECTOR_KEY;
import static com.datasqrl.io.tables.TableConfig.METADATA_COLUMN_ATTRIBUTE_KEY;
import static com.datasqrl.io.tables.TableConfig.METADATA_COLUMN_TYPE_KEY;
import static com.datasqrl.io.tables.TableConfig.METADATA_KEY;
import static com.datasqrl.io.tables.TableConfig.TABLE_KEY;
import static com.datasqrl.loaders.ObjectLoaderImpl.UDF_FUNCTION_CLASS;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.local.generate.AbstractTableNamespaceObject;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.ImportedRelationalTableImpl;
import com.datasqrl.plan.table.PrimaryKey;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqrlSqlValidator;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.functions.UserDefinedFunction;

@Slf4j
public class FlinkSqlNamespaceObject extends AbstractTableNamespaceObject {

  private final Set<SqlNode> nonTables;
  private final CalciteTableFactory tableFactory;
  private final SqrlFramework framework;
  private final SqlCreateTable sqlNode;

  public FlinkSqlNamespaceObject(SqrlFramework framework, SqlCreateTable sqlNode, Set<SqlNode> nonTables, CalciteTableFactory tableFactory,
      NameCanonicalizer canonicalizer, ModuleLoader moduleLoader) {
    super(tableFactory, canonicalizer, moduleLoader);
    this.framework = framework;
    this.sqlNode = sqlNode;
    this.nonTables = nonTables;
    this.tableFactory = tableFactory;
  }

  @Override
  public Name getName() {
    return Name.system(sqlNode.getTableName().names
        .get(sqlNode.getTableName().names.size()-1));
  }

  @Override
  public Object getTable() {
    return null;
  }

  @Override
  public boolean apply(Optional<String> alias, SqrlFramework framework, ErrorCollector errors) {
    //we will load the file (and functions?)
    //add functions to framework
    addFunctions(framework, nonTables);
    framework.getSchema().addAdditionalSql(nonTables);

    SqrlConfig config1 = toSqrlConfig(sqlNode);

    NamePath namePath = NamePath.of(sqlNode.getTableName().names.toArray(String[]::new));

    TableSource tableSource = getSource();

    RelDataType relDataType = createFields(sqlNode.getColumnList().getList());

    ImportedRelationalTableImpl importedTable = new ImportedRelationalTableImpl(namePath.getLast(),
        relDataType, tableSource);

    Optional<Integer> timestamp = config1.getSubConfig("table").asString("timestamp")
        .getOptional()
        .map((f)-> relDataType.getFieldNames().indexOf(f));

    List<String> pks = config1.getSubConfig("table").asList("primary-key", String.class).get();
    PrimaryKey key;
    if (!pks.isEmpty()) {
      int[] pkIndexs = relDataType.getFieldList().stream()
          .filter(f -> pks.contains(f.getName()))
          .mapToInt(RelDataTypeField::getIndex)
          .toArray();

      key = new PrimaryKey(pkIndexs);
    } else {
      key = new PrimaryKey(new int[]{});
    }
    //if has watermark, assure watermark is in table def
    ProxyImportRelationalTable proxyTable = tableFactory
        .createProxyTable(relDataType, namePath, importedTable, TableType.STREAM, timestamp, key);

    registerScriptTable(proxyTable, framework, Optional.empty(), Optional.empty());

    return true;
  }

  public TableSource getSource() {
    NamePath namePath = NamePath.of(sqlNode.getTableName().names.toArray(String[]::new));

    return new TableSource(new TableConfig(namePath.getLast(), getSqrlConfig()), namePath, namePath.getLast(), null);
  }

  @SneakyThrows
  private void addFunctions(SqrlFramework framework, Set<SqlNode> nonTables) {
    for (SqlNode statement : nonTables) {
      if (statement instanceof SqlCreateFunction) {
        SqlCreateFunction fnc = (SqlCreateFunction) statement;
        Class<?> functionClass = Class.forName(fnc.getFunctionClassName().toValue(), true,
            getClass().getClassLoader());
        Preconditions.checkArgument(UDF_FUNCTION_CLASS.isAssignableFrom(functionClass), "Class is not a UserDefinedFunction");

        UserDefinedFunction udf = (UserDefinedFunction) functionClass.getDeclaredConstructor().newInstance();

        // Return a namespace object containing the created function
        FlinkUdfNsObject flinkUdfNsObject = new FlinkUdfNsObject(
            getFunctionName(udf), udf, Optional.empty());
        flinkUdfNsObject.apply(Optional.of(fnc.getFunctionIdentifier()[fnc.getFunctionIdentifier().length-1]), framework, ErrorCollector.root());
      }

    }
  }

  private RelDataType createFields(List<SqlNode> list) {
    RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(framework.getTypeFactory());
    SqrlSqlValidator validator = (SqrlSqlValidator) framework.getQueryPlanner().createSqlValidator();

    //Dupe of TableConverter, just in a different context
    for (SqlNode f : list) {
      SqlDataTypeSpec type;
      if (f instanceof SqlMetadataColumn) {
        SqlMetadataColumn column = (SqlMetadataColumn) f;
        type = column.getType();
        RelDataType relDataType = type.deriveType(validator);
        typeBuilder.add(column.getName().getSimple(), relDataType);
      } else if (f instanceof SqlComputedColumn) {
        SqlComputedColumn column = (SqlComputedColumn) f;

        try {
          RelDataType relDataType = validator.inferReturnType(typeBuilder.build(),
              (SqlCall) column.getExpr(),
              framework.getCatalogReader());
          typeBuilder.add(column.getName().getSimple(), relDataType);
        } catch (Exception e) {
          throw new RuntimeException(
              String.format("Could not evaluate metadata expression. Reason: %s", e.getMessage()));
        }
      } else {
        SqlRegularColumn column = (SqlRegularColumn) f;
        type = column.getType();
        RelDataType relDataType = type.deriveType(validator);
        typeBuilder.add(column.getName().getSimple(), relDataType);
      }

    }

    return typeBuilder.build();
  }

  public SqrlConfig getSqrlConfig() {
    return toSqrlConfig(sqlNode);
  }

  private SqrlConfig toSqrlConfig(SqlCreateTable sqlNode) {
    //Construct sqrl config
    SqrlConfig config = SqrlConfig.create(ErrorCollector.root(), 1);
    SqrlConfig connector = config.getSubConfig(CONNECTOR_KEY);
    sqlNode.getPropertyList().getList()
        .stream()
        .map(o->(SqlTableOption)o)
        .forEach(o->connector.setProperty(o.getKeyString(), o.getValueString()));

    SqrlConfig metadata = config.getSubConfig(METADATA_KEY);

    sqlNode.getColumnList().getList().stream()
        .filter(f->f instanceof SqlMetadataColumn)
        .map(f->(SqlMetadataColumn)f)
        .forEach(f->metadata.getSubConfig(f.getName().getSimple())
            .setProperty(METADATA_COLUMN_TYPE_KEY, f.getType())
            .setProperty(METADATA_COLUMN_ATTRIBUTE_KEY, f.getMetadataAlias().get()));

    sqlNode.getColumnList().getList().stream()
        .filter(f->f instanceof SqlComputedColumn)
        .map(f->(SqlComputedColumn)f)
        .forEach(f->metadata.getSubConfig(f.getName().getSimple())
            .setProperty(METADATA_COLUMN_ATTRIBUTE_KEY, f.getExpr().toSqlString(FlinkDialect.DEFAULT)));

    SqrlConfig table = config.getSubConfig(TABLE_KEY);

    //look for pk
    if (!sqlNode.getTableConstraints().isEmpty()) {
      SqlTableConstraint sqlTableConstraint = sqlNode.getTableConstraints().get(0);
      table.setProperty(PRIMARYKEY_KEY, List.of(sqlTableConstraint.getColumnNames()));
    }
    if (sqlNode.getWatermark().isPresent()) {
      SqlWatermark watermark = sqlNode.getWatermark().get();
      table.setProperty(TIMESTAMP_COL_KEY, watermark.getEventTimeColumnName().getSimple());
      SqlNode watermarkStrategy = watermark.getWatermarkStrategy();
      extractBoundedStrategy(table, watermarkStrategy);
    }

    table.setProperty(TYPE_KEY, "source_and_sink");

    return config;
  }

  private void extractBoundedStrategy(SqrlConfig table, SqlNode watermarkStrategy) {
    try {
      //make strong assumptions until we can just pass literal sql
      SqlCall call = (SqlCall) watermarkStrategy;
      SqlLiteral literal = (SqlLiteral) call.getOperandList().get(1);
      IntervalValue value = (IntervalValue)literal.getValue();
      table.setProperty(WATERMARK_KEY, Double.parseDouble(value.getIntervalLiteral()) * 1000);
    } catch (Exception e) {
      log.warn("Could not parse watermark", e);
      e.printStackTrace();
      //ignore
    }
  }
}
