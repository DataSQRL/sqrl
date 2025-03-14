package com.datasqrl.v2.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;

/**
 * A builder for tables in Flink that is used as the central table builder in
 * DataSQRL. It's a light wrapper around the SqlNode relying heavily on {@link FlinkSqlNodeFactory}
 * for translation.
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

  public String getTableName() {
    return tableName.getSimple();
  }

  public FlinkTableBuilder setRelDataType(RelDataType relDataType) {
    setColumnList(FlinkSqlNodeFactory.createColumns(relDataType));
    return this;
  }

  public FlinkTableBuilder addComputedColumn(String columnName, SqlOperator operator) {
    Preconditions.checkArgument(columnList!=null, "Need to initialize columns first");
    columnList.add(FlinkSqlNodeFactory.getComputedColumn(columnName,
        FlinkSqlNodeFactory.getCallWithNoArgs(operator)));
    return this;
  }

  public List<String> extractMetadataColumns(String metadataAlias, boolean convertToRegular) {
    Preconditions.checkArgument(columnList!=null, "Need to initialize columns first");
    List<String> convertedColumns = new ArrayList<>();
    for (int i = 0; i < columnList.size(); i++) {
      SqlNode column = columnList.get(i);
      if (column instanceof SqlTableColumn.SqlMetadataColumn) {
        SqlTableColumn.SqlMetadataColumn columnMetadata = (SqlTableColumn.SqlMetadataColumn) column;
        if (columnMetadata.getMetadataAlias().filter(alias -> alias.equalsIgnoreCase(metadataAlias)).isPresent()) {
          convertedColumns.add(columnMetadata.getName().getSimple());
          if (convertToRegular) {
            SqlTableColumn.SqlRegularColumn regularColumn = new SqlRegularColumn(columnMetadata.getParserPosition(),
                columnMetadata.getName(), columnMetadata.getComment().orElse(null), columnMetadata.getType(), null);
            columnList.set(i, regularColumn);
          }
        }
      }
    }
    return convertedColumns;
  }

  public FlinkTableBuilder setConnectorOptions(Map<String, Object> options) {
    setPropertyList(FlinkSqlNodeFactory.createProperties(options));
    return this;
  }

  public Map<String, String> getConnectorOptions() {
    return FlinkSqlNodeFactory.propertiesToMap(getPropertyList());
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

  public boolean hasPartition() {
    return !getPartitionKeyList().isEmpty();
  }

  public List<String> getPartition() {
    return StreamUtil.filterByClass(getPartitionKeyList().getList(), SqlIdentifier.class).map(SqlIdentifier::getSimple)
            .collect(Collectors.toList());
  }

  public FlinkTableBuilder setPrimaryKey(List<String> pkColumns) {
    if (pkColumns.isEmpty()) {
      setTableConstraints(List.of());
    }
    SqlTableConstraint pkConstraint = FlinkSqlNodeFactory.createPrimaryKeyConstraint(pkColumns);
    setTableConstraints(Collections.singletonList(pkConstraint));
    return this;
  }

  public FlinkTableBuilder removePrimaryKey() {
    setTableConstraints(Collections.emptyList());
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
