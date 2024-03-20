package com.datasqrl.engine.stream.flink.plan;

import static com.datasqrl.io.tables.TableConfig.Base.*;
import static com.datasqrl.io.tables.TableConfig.CONNECTOR_KEY;
import static com.datasqrl.io.tables.TableConfig.METADATA_COLUMN_ATTRIBUTE_KEY;
import static com.datasqrl.io.tables.TableConfig.METADATA_COLUMN_TYPE_KEY;
import static com.datasqrl.io.tables.TableConfig.METADATA_KEY;
import static com.datasqrl.io.tables.TableConfig.TABLE_KEY;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.stream.flink.sql.calcite.FlinkDialect;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.table.ImportedRelationalTableImpl;
import com.datasqrl.util.CalciteUtil.RelDataTypeFieldBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class SqrlToFlinkSqlPlannerTest {

  @SneakyThrows
  @Test
  public void readConf() {
    String str = Files.readString(
        Path.of("../sqrl-examples/avro/ecommerce-avro/orders.sql"));

    SqlConformance conformance =  FlinkSqlConformance.DEFAULT;
    Config config = SqlParser.config()
        .withParserFactory(FlinkSqlParserFactories.create(conformance))
        .withConformance(conformance)
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256);
    SqlParser parser = SqlParser.create(str, config);
    SqlNode sqlNode = parser.parseStmt();
    SqlCreateTable createTable = (SqlCreateTable) sqlNode;
    SqrlConfig config1 = toSqrlConfig(createTable);

    Name orders = Name.system("orders");

    TableSource tableSource = new TableSource(new TableConfig(orders, config1), orders.toNamePath(), orders, null);

    SqrlFramework framework = new SqrlFramework();

    List<RelDataTypeField> fields = createTable.getColumnList().getList()
        .stream()
        .map(this::toField)
        .collect(Collectors.toList());

    RelDataTypeFieldBuilder fieldBuilder = new RelDataTypeFieldBuilder(new FieldInfoBuilder(framework.getTypeFactory()));
    fieldBuilder.addAll(fields);

    RelDataType relDataType = fieldBuilder.build();
    ImportedRelationalTableImpl importedRelationalTable = new ImportedRelationalTableImpl(orders,
        relDataType, tableSource);
    SqrlToFlinkSqlGenerator planner = new SqrlToFlinkSqlGenerator(framework);

    SqlCreateTable createTable1 = planner.toCreateTable("orders",
        importedRelationalTable.getRowType(), importedRelationalTable.getTableSource().getConfiguration(),
        true);

  }

  private RelDataTypeField toField(SqlNode f) {

    String name;
    SqlDataTypeSpec type;
    if (f instanceof SqlMetadataColumn) {
      SqlMetadataColumn column = (SqlMetadataColumn) f;
      type = column.getType();
      name = column.getName().getSimple();

    } else if (f instanceof SqlComputedColumn) {
      SqlComputedColumn column = (SqlComputedColumn) f;
      throw new RuntimeException();
    } else {
      SqlRegularColumn column = (SqlRegularColumn) f;
      type = column.getType();
      name = column.getName().getSimple();
    }
    SqrlFramework framework = new SqrlFramework();
    SqlValidator validator = framework.getQueryPlanner().createSqlValidator();

    RelDataType relDataType = type.deriveType(validator);

    return new RelDataTypeFieldImpl(name, -1, relDataType);
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
            .setProperty(METADATA_COLUMN_TYPE_KEY, null) //todo computed columns shouldn't need types
            .setProperty(METADATA_COLUMN_ATTRIBUTE_KEY, f.getExpr().toSqlString(FlinkDialect.DEFAULT)));

    SqlWatermark sqlWatermark = sqlNode.getWatermark()
        .get();

    //look for pk
    SqlTableConstraint sqlTableConstraint = sqlNode.getTableConstraints().get(0);

    SqrlConfig table = config.getSubConfig(TABLE_KEY)
        .setProperty(PRIMARYKEY_KEY, List.of(sqlTableConstraint.getColumnNames()))
        .setProperty(TIMESTAMP_COL_KEY, sqlWatermark.getEventTimeColumnName().getSimple())
        .setProperty(TYPE_KEY, "source_and_sink")
        .setProperty(WATERMARK_KEY, "5");

    return config;
  }
}