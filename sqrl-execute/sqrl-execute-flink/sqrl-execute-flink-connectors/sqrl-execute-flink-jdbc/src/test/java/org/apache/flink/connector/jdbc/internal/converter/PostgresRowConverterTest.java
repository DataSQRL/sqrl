package org.apache.flink.connector.jdbc.internal.converter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

class PostgresRowConverterTest {
  private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:15.4");

  @BeforeAll
  public static void setUp() {
    postgres.start();
  }

  @AfterAll
  public static void tearDown() {
    postgres.stop();
  }

  private ArrayType doubleArrayType = new ArrayType(new DoubleType());
  private ArrayType timestampArrayType = new ArrayType(new LocalZonedTimestampType());
  private ArrayType doubleArray2DType = new ArrayType(doubleArrayType);
  private RowType sampleRowType = RowType.of(new IntType(), new VarCharType());

  private void executeUpdate(Connection connection, String query) throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeUpdate(query);
    }
  }

  @Test
  public void testArraySerializationAndDeserialization() throws Exception {
    try (Connection connection = postgres.createConnection("")) {
      executeUpdate(connection, "CREATE TABLE test (id int, int_data int[], double_data double precision[], ts_data timestamptz[], double_data_2d double precision[][], row_data bytea)");

      // Set up the converter
      RowType rowType = RowType.of(new IntType(), new ArrayType(new IntType()), doubleArrayType, timestampArrayType, doubleArray2DType, sampleRowType);
      PostgresRowConverter converter = new PostgresRowConverter(rowType);

      // Sample data
      GenericRowData rowData = new GenericRowData(6);
      rowData.setField(0, 1);

      // Integer Array - GenericArrayData
      GenericArrayData intArray = new GenericArrayData(new int[]{1, 2, 3});
      rowData.setField(1, intArray);

      // Double Array - GenericArrayData
      GenericArrayData doubleArray = new GenericArrayData(new double[]{1.1, 2.2, 3.3});
      rowData.setField(2, doubleArray);

      // Timestamp Array - GenericArrayData
      BinaryArrayData array = new BinaryArrayData();
      BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);
      final int precision = 3;
      writer.reset();
      writer.writeTimestamp(0, TimestampData.fromEpochMillis(123000L), precision);
      writer.writeTimestamp(1, TimestampData.fromEpochMillis(123000L), precision);
      writer.complete();
      rowData.setField(3, array);

      // 2D Double Array - GenericArrayData
      GenericArrayData doubleArray2d = new GenericArrayData(new double[][]{{1.1, 2.2}, {3.3, 4.4}});
      rowData.setField(4, doubleArray2d);

      // RowType not being an array
      GenericRowData sampleRow = new GenericRowData(2);
      sampleRow.setField(0, 10);
      sampleRow.setField(1, "test");
      rowData.setField(5, sampleRow);

      FieldNamedPreparedStatement statement =
          FieldNamedPreparedStatementImpl.prepareStatement(connection,
              "INSERT INTO test (id, int_data, double_data, ts_data, double_data_2d, row_data) VALUES (:id, :int_data, :double_data, :ts_data, :double_data_2d, :row_data)",
              List.of("id", "int_data", "double_data", "ts_data", "double_data_2d", "row_data").toArray(String[]::new));

      for (int i = 0; i < rowType.getFieldCount(); i++) {
        JdbcSerializationConverter externalConverter =
            converter.createExternalConverter(rowType.getTypeAt(i));
        externalConverter.serialize(rowData, i, statement);
      }
      statement.addBatch();

      int[] result = statement.executeBatch();
      assertEquals(1, result.length);

      Statement stmt = connection.createStatement();
      // Deserialize
      ResultSet rs = stmt.executeQuery("SELECT int_data, double_data, ts_data, double_data_2d, row_data FROM test WHERE id=1");
      assertTrue(rs.next());

      // Assert Integer Array
      Array intArrayRetrieved = rs.getArray("int_data");
      Object intDataDeserialized = converter.createArrayConverter(new ArrayType(new IntType()))
          .deserialize(intArrayRetrieved);
      assertArrayEquals(((GenericArrayData)rowData.getField(1)).toIntArray(),
          ((GenericArrayData)intDataDeserialized).toIntArray());

      // Assert Double Array
      Array doubleArrayRetrieved = rs.getArray("double_data");
      Object doubleDataDeserialized = converter.createArrayConverter(doubleArrayType)
          .deserialize(doubleArrayRetrieved);
      assertArrayEquals(((GenericArrayData)rowData.getField(2)).toDoubleArray(),
          ((GenericArrayData)doubleDataDeserialized).toDoubleArray());

      // Assert Timestamp Array
      Array timestampArrayRetrieved = rs.getArray("ts_data");
      Object timestampDataDeserialized = converter.createArrayConverter(timestampArrayType)
          .deserialize(timestampArrayRetrieved);
      assertArrayEquals(((GenericArrayData) timestampDataDeserialized).toObjectArray(),
          Arrays.stream(((BinaryArrayData) rowData.getField(3)).toObjectArray(new TimestampType())).toArray()
          );
      // Assert 2D Double Array (it's a bit tricky given the 2D nature)
      Array double2DArrayRetrieved = rs.getArray("double_data_2d");
      Object double2DDataDeserialized = converter.createArrayConverter(doubleArray2DType)
          .deserialize(double2DArrayRetrieved);
      //todo: 2d arrays are not well supported
      GenericArrayData field = (GenericArrayData) rowData.getField(4);
      assertNotNull(field);

      //todo: Row type not well supported
      Object rowRetrieved = rs.getObject("row_data");
      assertNotNull(rowRetrieved);
    }
  }
}