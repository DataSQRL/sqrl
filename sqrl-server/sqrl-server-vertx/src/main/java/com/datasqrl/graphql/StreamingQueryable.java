package com.datasqrl.graphql;

import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import org.apache.calcite.linq4j.BaseQueryable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.table.jdbc.DriverUri;
import org.apache.flink.table.jdbc.FlinkConnection;
import org.apache.flink.table.jdbc.FlinkResultSet;
import org.apache.flink.table.jdbc.FlinkStatement;

public class StreamingQueryable<T> extends BaseQueryable<T> {

  protected StreamingQueryable(QueryProvider provider, Type elementType, Expression expression) {
    super(provider, elementType, expression);
  }

  public static <T> Iterator<T> iterator2() {

    Properties properties = new Properties();
    properties.setProperty(RestOptions.CONNECTION_TIMEOUT.key(), "0");
    properties.setProperty(RestOptions.IDLENESS_TIMEOUT.key(), "0");
//        properties.setProperty(RestOptions.CLIENT_MAX_CONTENT_LENGTH.key(), "-1");
    try (FlinkConnection connection = new FlinkConnection(
        DriverUri.create("jdbc:flink://localhost:8083", properties))) {
      // create databases in default catalog
//            AbstractNioChannel.config().setConnectTimeoutMillis(-1);
      FlinkStatement statement = (FlinkStatement) connection.createStatement();
      statement.execute(
          "CREATE TABLE RandomData (\n" + " id INT, \n" + " text STRING \n" + ") WITH (\n"
              + " 'connector' = 'datagen',\n" + " 'rows-per-second'='1',\n"
//                + " 'number-of-rows'='1000000',\n"
              + " 'fields.id.kind'='random',\n" + " 'fields.id.min'='1',\n"
              + " 'fields.id.max'='1000',\n" + " 'fields.text.length'='10'\n" + ")");

      final FlinkResultSet resultSet = (FlinkResultSet) statement.executeQuery(
          "SELECT * FROM RandomData");

      return new Iterator<T>() {
        private T nextElement = null;
        private boolean done = false;

        @Override
        public boolean hasNext() {
          if (done) {
            return false;
          }
          if (nextElement != null) {
            return true;
          }

          try {
            if (resultSet.next()) {
              // Assuming each row is returned as an Object array
              nextElement = (T) new Object[]{resultSet.getInt("id"), resultSet.getString("text")};
              return true;
            } else {
              close();
              return false;
            }
          } catch (SQLException e) {
            close();
            throw new RuntimeException("Error fetching next element.", e);
          }
        }

        @Override
        public T next() {
          if (nextElement != null || hasNext()) {
            T result = nextElement;
            nextElement = null;
            return result;
          }
          throw new NoSuchElementException();
        }

        private void close() {
          done = true;
          try {
            resultSet.close();
            statement.close();
            connection.close();
          } catch (SQLException e) {
            throw new RuntimeException("Error closing streaming resources.", e);
          }
        }
      };

//          try {
//            while (resultSet.next()) {
//              System.out.println(
//                  "ID: " + resultSet.getInt("id") + ", Text: " + resultSet.getString("text"));
//            }
//
//          } catch (Exception e) {
//            e.printStackTrace();
//          }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public Iterator<T> iterator() {
    return null;
  }
  // Implement streaming logic here, possibly using Flink DataStream API
}
