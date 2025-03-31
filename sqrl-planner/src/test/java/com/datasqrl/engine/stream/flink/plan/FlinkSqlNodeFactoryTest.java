package com.datasqrl.engine.stream.flink.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.config.TableConfig.MetadataEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.junit.jupiter.api.Test;

public class FlinkSqlNodeFactoryTest {

  @Value
  public static class MockMetadataEntry implements MetadataEntry {
    private final Optional<String> type;
    private final Optional<String> attribute;
    private final Optional<Boolean> virtual;
  }

  private String unparse(SqlNode node) {
    return QueryPlanner.sqlToString(Dialect.FLINK, ()->node).getSql();

  }
  @Test
  void testCreateView() {
    String tableName = "my_view";
    SqlNode fromTable = FlinkSqlNodeFactory.identifier("source_table");
    SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    SqlSelect select = new SqlSelect(SqlParserPos.ZERO, null, selectList, fromTable, null, null, null, null, null, null, null, null);

    SqlCreateView createView = FlinkSqlNodeFactory.createView(tableName, select);
    String sql = unparse(createView);
    String expectedSql = "CREATE VIEW `my_view`\n"
        + "AS\n"
        + "SELECT `*`\n"
        + "FROM `source_table`";
    assertEquals(expectedSql, sql.trim());
  }

  @Test
  void testCreateInsert() {
    String targetTable = "target_table";
    SqlNode fromTable = FlinkSqlNodeFactory.identifier("source_table");
    SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    SqlSelect select = new SqlSelect(SqlParserPos.ZERO, null, selectList, fromTable, null, null, null, null, null, null, null, null);

    RichSqlInsert insert = FlinkSqlNodeFactory.createInsert(select, targetTable);
    String sql = unparse(insert);
    String expectedSql = "INSERT INTO `target_table`\n"
        + "(SELECT `*`\n"
        + " FROM `source_table`)";
    assertEquals(expectedSql, sql.trim());
  }

  @Test
  void testCreateFunction() {
    String functionName = "my_udf";
    String className = "com.example.MyUDF";
    SqlCreateFunction createFunction = FlinkSqlNodeFactory.createFunction(functionName, className, false);

    String sql = unparse(createFunction);
    String expectedSql = "CREATE FUNCTION IF NOT EXISTS `my_udf` AS 'com.example.MyUDF' LANGUAGE JAVA";
    assertEquals(expectedSql, sql.trim());
  }

  @Test
  void testCreateWatermark() {
    String eventTimeColumn = "timestamp_col";
    SqlIdentifier eventTimeIdentifier = FlinkSqlNodeFactory.identifier(eventTimeColumn);
    String delay = "5";
    SqlNode watermarkStrategy = FlinkSqlNodeFactory.boundedStrategy(eventTimeIdentifier, delay);

    SqlWatermark watermark = FlinkSqlNodeFactory.createWatermark(eventTimeIdentifier, watermarkStrategy);
    String sql = unparse(watermark);
    String expectedSql = "WATERMARK FOR `timestamp_col` AS `timestamp_col` - INTERVAL '5' SECOND";
    assertEquals(expectedSql, sql.trim());
  }

  @Test
  void testBoundedStrategy() {
    SqlIdentifier watermark = FlinkSqlNodeFactory.identifier("timestamp_col");
    String delay = "5";

    SqlNode boundedStrategy = FlinkSqlNodeFactory.boundedStrategy(watermark, delay);
    String sql = unparse(boundedStrategy);
    String expectedSql = "`timestamp_col` - INTERVAL '5' SECOND";
    assertEquals(expectedSql, sql.trim());
  }

  @Test
  void testCreatePrimaryKeyConstraint() {
    List<String> primaryKeyColumns = Arrays.asList("id", "timestamp_col");
    SqlTableConstraint pkConstraint = FlinkSqlNodeFactory.createPrimaryKeyConstraint(primaryKeyColumns);
    String sql = unparse(pkConstraint);
    String expectedSql = "PRIMARY KEY (`id`, `timestamp_col`) NOT ENFORCED";
    assertEquals(expectedSql, sql.trim());
  }

  @Test
  void testCreateProperties() {
    Map<String, Object> options = new HashMap<>();
    options.put("connector", "kafka");
    options.put("topic", "my_topic");
    options.put("format", "json");

    SqlNodeList properties = FlinkSqlNodeFactory.createProperties(options);
    String sql = unparse(properties);
    String expectedSql = "'connector' = 'kafka', 'format' = 'json', 'topic' = 'my_topic'";
    assertEquals(expectedSql, sql.trim());
  }

  @Test
  void testCreatePartitionKeys() {
    List<String> partitionKeys = Arrays.asList("year", "month", "day");
    SqlNodeList partitionKeysNode = FlinkSqlNodeFactory.createPartitionKeys(partitionKeys);
    String sql = unparse(partitionKeysNode);
    String expectedSql = "`year`, `month`, `day`";
    assertEquals(expectedSql, sql.trim());
  }

  @Test
  void testCreateTable() {
    String tableName = "my_table";
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // Define the schema
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 255);
    RelDataType timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);

    RelDataType rowType = typeFactory.builder()
        .add("id", intType)
        .add("name", varcharType)
        .add("timestamp_col", timestampType)
        .add("metadata_col1", varcharType)
        .add("metadata_col2", varcharType)
        .build();

    Optional<List<String>> partitionKeys = Optional.of(Arrays.asList("name"));
    long watermarkMillis = 5000;
    Optional<String> timestampColumn = Optional.of("timestamp_col");
    Map<String, MetadataEntry> metadataConfig = new HashMap<>();

    // MetadataEntry with type only
    MetadataEntry metadataEntry1 = new MockMetadataEntry(Optional.of("timestamp_col"), Optional.empty(), Optional.empty());
    metadataConfig.put("metadata_col1", metadataEntry1);

    // MetadataEntry with attribute
    MetadataEntry metadataEntry2 = new MockMetadataEntry(Optional.of("timestamp_col"), Optional.of("timestamp"), Optional.of(true));
    metadataConfig.put("metadata_col2", metadataEntry2);

    List<String> primaryKeyConstraint = Arrays.asList("id");
    Map<String, Object> connectorProperties = new HashMap<>();
    connectorProperties.put("connector", "filesystem");
    connectorProperties.put("path", "/tmp/data");
    connectorProperties.put("format", "csv");

    // Simple MetadataExpressionParser implementation
    FlinkSqlNodeFactory.MetadataExpressionParser expressionParser = expression -> {
      // Return an identifier for simplicity
      return FlinkSqlNodeFactory.identifier(expression);
    };

    SqlCreateTable createTable = FlinkSqlNodeFactory.createTable(
        tableName,
        rowType,
        partitionKeys,
        watermarkMillis,
        timestampColumn,
        metadataConfig,
        primaryKeyConstraint,
        connectorProperties,
        expressionParser
    );
    // Unparse and compare SQL strings
    String sql = unparse(createTable);
    String expectedSql = "CREATE TABLE `my_table` (\n"
        + "  `id` INTEGER NOT NULL,\n"
        + "  `name` VARCHAR(255) CHARACTER SET `UTF-16LE` NOT NULL,\n"
        + "  `timestamp_col` TIMESTAMP(0) NOT NULL,\n"
        + "  `metadata_col1` VARCHAR(255) CHARACTER SET `UTF-16LE` NOT NULL METADATA FROM 'timestamp_col',\n"
        + "  `metadata_col2` VARCHAR(255) CHARACTER SET `UTF-16LE` NOT NULL METADATA FROM 'timestamp' VIRTUAL,\n"
        + "  PRIMARY KEY (`id`) NOT ENFORCED,\n"
        + "  WATERMARK FOR `timestamp_col` AS `timestamp_col` - INTERVAL '5.0' SECOND\n"
        + ")\n"
        + "PARTITIONED BY (`name`)\n"
        + "WITH (\n"
        + "  'format' = 'csv',\n"
        + "  'path' = '/tmp/data',\n"
        + "  'connector' = 'filesystem'\n"
        + ")";
    assertEquals(expectedSql.trim(), sql.trim());
  }
}
