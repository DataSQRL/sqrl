package ai.dataeng.sqml.analyzer2;

import static org.apache.flink.table.api.Expressions.$;

import ai.dataeng.sqml.tree.name.Name;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.StatementSetImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

public class SqrlSinkBuilder {

  private final TableEnvironment env;
  private final TableManager tableManager;

  public SqrlSinkBuilder(TableEnvironment env, TableManager tableManager) {
    this.env = env;
    this.tableManager = tableManager;
  }

  @SneakyThrows
  public void build() {


    StatementSetImpl set = (StatementSetImpl) env.createStatementSet();

    for (Map.Entry<Name, SqrlEntity> entityEntry : tableManager.getTables().entrySet()) {
      Name name = entityEntry.getKey();
      Name tableName = Name.system(name.getCanonical().replaceAll("\\.", "_") + "_flink");
      SqrlEntity entity = entityEntry.getValue();
      String sql = "CREATE TABLE %s("
          + entity.getTable().getResolvedSchema()
          .getColumns()
          .stream()
          .map(c -> "`" + c.getName() + "`" + " " + getType(c.getDataType().getLogicalType()))
          .collect(Collectors.joining(", "))
          + getPK(entity)
          + ") WITH ( "
//              + "'connector' = 'print'"
          + "'connector' = 'jdbc',"
          + "'url'='jdbc:postgresql://localhost:5432/henneberger',"
////              + "'default-database' = 'henneberger',"
          + "'table-name' = '%s'"
////              + "'username' = 'henneberger',"
////              + "'password' = ''"
          + ")";
      System.out.println(tableName.getDisplay());
      env.executeSql(
          String.format(sql, tableName.getDisplay(), tableName.getDisplay())
      );

//    System.out.println(sql);

      String drop = "DROP TABLE IF EXISTS %s;";
      String postgresSql = "CREATE TABLE %s(" +
          entity.getTable().getResolvedSchema()
              .getColumns()
              .stream().map(
                  c -> "\"" + c.getName() + "\"" + " " + getSqlType(c.getDataType().getLogicalType()))
              .collect(Collectors.joining(", "))
          + getPostgresPK(entity)
          + ")";

      JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:postgresql://localhost:5432/henneberger")
          .build();
      Connection conn = getConnection(jdbcOptions);
      conn.createStatement().execute(String.format(drop, tableName.getDisplay()));
      conn.createStatement().execute(String.format(postgresSql, tableName.getDisplay()));

//      env.("INSERT INTO "+tableName.getDisplay()+" SELECT * FROM "+entity.getTable());
//      System.out.println();

      set.addInsert(tableName.getDisplay(), entity.getTable());
    }


    //Can throw an exception if batch
    System.out.println(set.getJsonPlan());
    TableResult result = set.execute();
    System.out.println(result);

    try {
      result.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  private String getPK(SqrlEntity entity1) {
    if (!entity1.getPrimaryKey().isEmpty()) {

      return ", PRIMARY KEY ("+
          entity1.getPrimaryKey().stream()
              .map(Name::getCanonical)
              .collect(Collectors.joining(", "))
          +") NOT ENFORCED";
    }
    return "";
  }
  private String getPostgresPK(SqrlEntity entity1) {
    if (!entity1.getPrimaryKey().isEmpty()) {

      return ", PRIMARY KEY ("+
          entity1.getPrimaryKey().stream()
              .map(Name::getCanonical)
              .collect(Collectors.joining(", "))
          +")";
    }
    return "";
  }

  public static Connection getConnection(JdbcConnectionOptions jdbcOptions) throws SQLException, ClassNotFoundException {
    return DriverManager.getConnection(jdbcOptions.getDbURL(),
        jdbcOptions.getUsername().orElse(null),
        jdbcOptions.getPassword().orElse(null));

  }

  private String getType(LogicalType logicalType) {

    return logicalType.accept(new TypeConverter());
  }

  public class TypeConverter implements LogicalTypeVisitor<String> {

    @Override
    public String visit(CharType charType) {
      return "String";
    }

    @Override
    public String visit(VarCharType varCharType) {
      return "String";
    }

    @Override
    public String visit(BooleanType booleanType) {
      return "BOOLEAN";
    }

    @Override
    public String visit(BinaryType binaryType) {
      return "BYTES";
    }

    @Override
    public String visit(VarBinaryType varBinaryType) {
      return "BYTES";
    }

    @Override
    public String visit(DecimalType decimalType) {
      return "DECIMAL";
    }

    @Override
    public String visit(TinyIntType tinyIntType) {
      return "INT";
    }

    @Override
    public String visit(SmallIntType smallIntType) {
      return "INT";
    }

    @Override
    public String visit(IntType intType) {
      return "INT";
    }

    @Override
    public String visit(BigIntType bigIntType) {
      return "BIGINT";
    }

    @Override
    public String visit(FloatType floatType) {
      return "FLOAT";
    }

    @Override
    public String visit(DoubleType doubleType) {
      return "DOUBLE";
    }

    @Override
    public String visit(DateType dateType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(TimeType timeType) {
      return "TIME";
    }

    @Override
    public String visit(TimestampType timestampType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(ZonedTimestampType zonedTimestampType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(LocalZonedTimestampType localZonedTimestampType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(YearMonthIntervalType yearMonthIntervalType) {
      return "String";
    }

    @Override
    public String visit(DayTimeIntervalType dayTimeIntervalType) {
      return "String";
    }

    @Override
    public String visit(ArrayType arrayType) {
      return null;
    }

    @Override
    public String visit(MultisetType multisetType) {
      return null;
    }

    @Override
    public String visit(MapType mapType) {
      return null;
    }

    @Override
    public String visit(RowType rowType) {
      return null;
    }

    @Override
    public String visit(DistinctType distinctType) {
      return null;
    }

    @Override
    public String visit(StructuredType structuredType) {
      return null;
    }

    @Override
    public String visit(NullType nullType) {
      return null;
    }

    @Override
    public String visit(RawType<?> rawType) {
      return null;
    }

    @Override
    public String visit(SymbolType<?> symbolType) {
      return null;
    }

    @Override
    public String visit(LogicalType logicalType) {
      return null;
    }
  }
  private String getSqlType(LogicalType logicalType) {

    return logicalType.accept(new SqlTypeConverter());
  }

  public class SqlTypeConverter implements LogicalTypeVisitor<String> {

    @Override
    public String visit(CharType charType) {
      return "VARCHAR";
    }

    @Override
    public String visit(VarCharType varCharType) {
      return "VARCHAR";
    }

    @Override
    public String visit(BooleanType booleanType) {
      return "BOOLEAN";
    }

    @Override
    public String visit(BinaryType binaryType) {
      return "BYTES";
    }

    @Override
    public String visit(VarBinaryType varBinaryType) {
      return "BYTES";
    }

    @Override
    public String visit(DecimalType decimalType) {
      return "DECIMAL";
    }

    @Override
    public String visit(TinyIntType tinyIntType) {
      return "INT";
    }

    @Override
    public String visit(SmallIntType smallIntType) {
      return "INT";
    }

    @Override
    public String visit(IntType intType) {
      return "INT";
    }

    @Override
    public String visit(BigIntType bigIntType) {
      return "BIGINT";
    }

    @Override
    public String visit(FloatType floatType) {
      return "FLOAT";
    }

    @Override
    public String visit(DoubleType doubleType) {
      return "DOUBLE";
    }

    @Override
    public String visit(DateType dateType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(TimeType timeType) {
      return "TIME";
    }

    @Override
    public String visit(TimestampType timestampType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(ZonedTimestampType zonedTimestampType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(LocalZonedTimestampType localZonedTimestampType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(YearMonthIntervalType yearMonthIntervalType) {
      return "VARCHAR";
    }

    @Override
    public String visit(DayTimeIntervalType dayTimeIntervalType) {
      return "VARCHAR";
    }

    @Override
    public String visit(ArrayType arrayType) {
      return null;
    }

    @Override
    public String visit(MultisetType multisetType) {
      return null;
    }

    @Override
    public String visit(MapType mapType) {
      return null;
    }

    @Override
    public String visit(RowType rowType) {
      return null;
    }

    @Override
    public String visit(DistinctType distinctType) {
      return null;
    }

    @Override
    public String visit(StructuredType structuredType) {
      return null;
    }

    @Override
    public String visit(NullType nullType) {
      return null;
    }

    @Override
    public String visit(RawType<?> rawType) {
      return null;
    }

    @Override
    public String visit(SymbolType<?> symbolType) {
      return null;
    }

    @Override
    public String visit(LogicalType logicalType) {
      return null;
    }
  }
}
