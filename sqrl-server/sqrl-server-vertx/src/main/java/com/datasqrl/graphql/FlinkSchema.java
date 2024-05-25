package com.datasqrl.graphql;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.jdbc.DriverUri;
import org.apache.flink.table.jdbc.FlinkConnection;
import org.apache.flink.table.jdbc.FlinkResultSet;
import org.apache.flink.table.jdbc.FlinkStatement;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FlinkSchema extends AbstractSchema {

  @Override
  protected Map<String, Table> getTableMap() {
    return Map.of("sales_fact_1997", new FlinkTable());
  }

  private class FlinkTable extends AbstractTable implements ScannableTable, StreamableTable {


    @Override
    public Statistic getStatistic() {
      return Statistics.of(100d, List.of(), RelCollations.createSingleton(0));
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
      return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
        @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      return false;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
      return relDataTypeFactory.createStructType(StructKind.FULLY_QUALIFIED,
          List.of(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
              relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR)), List.of("customerid", "text"));
    }

    @Override
    public Table stream() {
      return this;
    }

    @SneakyThrows
    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {

      Properties properties = new Properties();
      properties.setProperty(RestOptions.CONNECTION_TIMEOUT.key(), "0");
      properties.setProperty(RestOptions.IDLENESS_TIMEOUT.key(), "0");
//        properties.setProperty(RestOptions.CLIENT_MAX_CONTENT_LENGTH.key(), "-1");
      FlinkConnection connection = new FlinkConnection(
          DriverUri.create("jdbc:flink://localhost:8083", properties));

      FlinkStatement statement = (FlinkStatement) connection.createStatement();
      statement.execute(
          "CREATE TABLE RandomData (\n" + " customerid INT, \n" + " text STRING \n" + ") WITH (\n"
              + " 'connector' = 'datagen',\n"
                + " 'number-of-rows'='10',\n"
              + " 'fields.customerid.kind'='random',\n" + " 'fields.customerid.min'='1',\n"
              + " 'fields.customerid.max'='1000',\n" + " 'fields.text.length'='10'\n" + ")");

      final FlinkResultSet resultSet = (FlinkResultSet) statement.executeQuery(
          "SELECT * FROM RandomData");
      return Linq4j.asEnumerable(() -> new Iterator<Object[]>() {
        private Object[] nextElement = null;
        private boolean fetched = false;

        @Override
        public boolean hasNext() {
          if (!fetched) {
            try {
              if (resultSet.next()) {
                nextElement = new Object[]{resultSet.getInt("customerid"), resultSet.getString("text")};
                fetched = true;
              } else {
                close();
                return false;
              }
            } catch (SQLException e) {
              close();
              throw new RuntimeException("Error fetching next element.", e);
            }
          }
          return fetched;
        }

        @Override
        public Object[] next() {
          if (!fetched && !hasNext()) {
            throw new NoSuchElementException();
          }
          fetched = false;
          return nextElement;
        }

        private void close() {
          try {
            resultSet.close();
            statement.close();
            connection.close();
          } catch (SQLException e) {
            throw new RuntimeException("Error closing streaming resources.", e);
          }
        }
      });
    }
  }
}
