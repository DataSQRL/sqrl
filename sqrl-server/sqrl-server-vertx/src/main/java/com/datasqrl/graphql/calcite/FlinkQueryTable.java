package com.datasqrl.graphql.calcite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
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
import org.apache.flink.table.jdbc.FlinkResultSet;
import org.apache.flink.table.jdbc.FlinkStatement;

class FlinkQueryTable extends AbstractTable implements ScannableTable
    , StreamableTable
{

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
    Connection connection = flinkSchema.getConnection();
    FlinkStatement statement = (FlinkStatement) connection.createStatement();
//    PreparedStatement preparedStatement = connection.prepareStatement(query.getQuery());
//    preparedStatement.setString(1, "0");
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
              System.out.println(Arrays.toString(nextElement));
              fetched = true;
            } else {
              System.out.println("closing HasNext");
              close();
              return false;
            }
          } catch (SQLException e) {
            e.printStackTrace();
            close();
            throw new RuntimeException("Error fetching next element.", e);
          }
        } else {
          System.out.println("no no next fetch");
        }
        return fetched;
      }

      @Override
      public Object[] next() {
        if (!fetched && !hasNext()) {
          new NoSuchElementException().printStackTrace();
          throw new NoSuchElementException();
        }
        System.out.println("no next");
        fetched = false;
        return nextElement;
      }

      private void close() {
        try {
          System.out.println("closing");

          resultSet.close();
          statement.close();
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
          throw new RuntimeException("Error closing streaming resources.", e);
        }
      }
    });
  }
}