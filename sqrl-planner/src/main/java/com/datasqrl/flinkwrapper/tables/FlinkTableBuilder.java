package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

/**
 * A builder for {@link FlinkTableConfig}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlinkTableBuilder {

  private SqlIdentifier tableName = null; //required, no default

  private SqlNodeList columnList = null; //required, no default

  private SqlNodeList propertyList = SqlNodeList.EMPTY;

  private List<SqlTableConstraint> tableConstraints = Collections.emptyList();

  private SqlNodeList partitionKeyList = SqlNodeList.EMPTY;

  private SqlWatermark watermark = null;

  public FlinkTableBuilder setName(Name name) {
    setName(name.getDisplay());
    return this;
  }

  public FlinkTableBuilder setName(String name) {
    setTableName(FlinkSqlNodeFactory.identifier(name));
    return this;
  }

  public FlinkTableBuilder setRelDataType(RelDataType relDataType) {
    setColumnList(FlinkSqlNodeFactory.createColumns(relDataType));
    return this;
  }

  public FlinkTableBuilder setConnectorOptions(Map<String, Object> options) {
    setPropertyList(FlinkSqlNodeFactory.createProperties(options));
    return this;
  }

  public FlinkTableBuilder setDummyConnector() {
    return setConnectorOptions(Map.of("connector","datagen"));
  }

  public FlinkTableBuilder setWatermarkMillis(String timestampColumnName, long watermarkMillis) {
    setWatermark(FlinkSqlNodeFactory.createWatermark(timestampColumnName, watermarkMillis));
    return this;
  }

  public FlinkTableBuilder setPartition(List<String> partitionColumns) {
    if (partitionColumns.isEmpty()) setPartitionKeyList(SqlNodeList.EMPTY);
    setPartitionKeyList(FlinkSqlNodeFactory.createPartitionKeys(partitionColumns));
    return this;
  }

  public FlinkTableBuilder setPrimaryKey(List<String> pkColumns) {
    if (pkColumns.isEmpty()) {
      setTableConstraints(List.of());
    }
    SqlTableConstraint pkConstraint = FlinkSqlNodeFactory.createPrimaryKeyConstraint(pkColumns);
    setTableConstraints(Collections.singletonList(pkConstraint));
    return this;
  }

  public Optional<List<String>> getPrimaryKey() {
    if (tableConstraints.isEmpty()) return Optional.empty();
    Preconditions.checkArgument(tableConstraints.size()==1, "Expected a single table constraint");
    SqlTableConstraint pkConstraint = tableConstraints.get(0);
    Preconditions.checkArgument(pkConstraint.isPrimaryKey(), "Expected a primary key constraint");
    return Optional.of(Arrays.asList(pkConstraint.getColumnNames()));
  }

  public boolean hasPrimaryKey() {
    return getPrimaryKey().isPresent();
  }


  public SqlCreateTable buildSql(boolean isTemporary) {
    return new SqlCreateTable(SqlParserPos.ZERO,
        tableName,
        columnList,
        tableConstraints,
        propertyList,
        partitionKeyList,
        watermark,
        null,
        isTemporary,
        false
        );
  }

  public static FlinkTableBuilder toBuilder(SqlCreateTable table) {
    return new FlinkTableBuilder(table.getTableName(),
        table.getColumnList(),
        table.getPropertyList(),
        table.getTableConstraints(),
        table.getPartitionKeyList(),
        table.getWatermark().orElse(null));
  }

}
