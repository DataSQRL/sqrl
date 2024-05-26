package com.datasqrl.graphql.calcite;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
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
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.jdbc.DriverUri;
import org.apache.flink.table.jdbc.FlinkConnection;
import org.apache.flink.table.jdbc.FlinkResultSet;
import org.apache.flink.table.jdbc.FlinkStatement;

class FlinkQueryTable extends AbstractTable implements ScannableTable, StreamableTable {

  private final FlinkSchema flinkSchema;
  private final FlinkQuery query;

  @SneakyThrows
  FlinkQueryTable(FlinkSchema flinkSchema, FlinkQuery query) {
    this.flinkSchema = flinkSchema;
    this.query = query;
  }

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
  public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
      CalciteConnectionConfig config) {
    return false;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return query.getRelDataType();
  }

  @Override
  public Table stream() {
    return this;
  }

  @SneakyThrows
  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    FlinkStatement statement = (FlinkStatement) flinkSchema.getConnection().createStatement();

    final FlinkResultSet resultSet = (FlinkResultSet)
        statement.executeQuery(query.getQuery());

    return Linq4j.asEnumerable(() -> new Iterator<Object[]>() {
      private Object[] nextElement = null;
      private boolean fetched = false;

      @Override
      public boolean hasNext() {
        if (!fetched) {
          try {
            if (resultSet.next()) {
              nextElement = new Object[]{resultSet.getInt("customerid"),
                  resultSet.getString("text")};
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
//          connection.close();
        } catch (SQLException e) {
          throw new RuntimeException("Error closing streaming resources.", e);
        }
      }
    });
  }
}