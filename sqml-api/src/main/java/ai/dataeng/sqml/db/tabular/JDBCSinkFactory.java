package ai.dataeng.sqml.db.tabular;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.type.*;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.jooq.*;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

public class JDBCSinkFactory implements TabularSinkFactory {

    private final JdbcConnectionOptions options;
    private final SQLDialect dialect;

    private transient SimpleJdbcConnectionProvider connectionProvider;

    public JDBCSinkFactory(JdbcConnectionOptions options, SQLDialect dialect) {
        Preconditions.checkArgument(dialect==SQLDialect.POSTGRES || dialect==SQLDialect.H2, "Only Postgres and H2 are supported currently");
        this.options = options;
        this.dialect = dialect;
    }

    private static final String sqlName(String name) {
        return "\"" + name + "\"";
    }

    @Override
    public SinkFunction<Row> getSink(String tableName, DestinationTableSchema schema) {
        try {
            createTable(tableName, schema);
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute SQL query",e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not load database driver",e);
        } catch (Exception e) {
            throw new RuntimeException("Encountered exception",e);
        }
        StringBuilder s = new StringBuilder();
        if (dialect == SQLDialect.H2) {
            s.append("MERGE INTO ").append(sqlName(tableName)).append(" KEY (");
            s.append(getPrimaryKeyNames(schema).stream().map(n -> sqlName(n)).collect(Collectors.joining(", ")));
            s.append(") VALUES (");
            s.append(Arrays.stream(new int[schema.length()]).mapToObj(i -> "?").collect(Collectors.joining(", ")));
            s.append(");");
        } else if (dialect == SQLDialect.POSTGRES) {
            throw new NotImplementedException("Not yet implemented");
        } else throw new UnsupportedOperationException();

        String sqlinsert=s.toString();
        System.out.println("Query: " + sqlinsert);
        return JdbcSink.sink(sqlinsert, new StatementBuilder(schema), JdbcExecutionOptions.defaults(), options);
    }

    public static class StatementBuilder implements JdbcStatementBuilder<Row> {

        private final DestinationTableSchema schema;

        public StatementBuilder(DestinationTableSchema schema) {
            this.schema = schema;
        }

        @Override
        public void accept(PreparedStatement ps, Row row) throws SQLException {
            Preconditions.checkArgument(row.getArity() == schema.length(), "Incompatible Row: %s",row);
            for (int i = 0; i < schema.length(); i++) {
                DestinationTableSchema.Field field = schema.get(i);
                if (field.isArray()) ps.setArray(i+1, row.getFieldAs(i));
                else {
                    ScalarType type = field.getType();
                    if (type instanceof StringType) ps.setString(i+1,row.getFieldAs(i));
                    else if (type instanceof IntegerType) ps.setLong(i+1,row.getFieldAs(i));
                    else if (type instanceof FloatType) ps.setDouble(i+1,row.getFieldAs(i));
                    else if (type instanceof NumberType) ps.setDouble(i+1,row.getFieldAs(i));
                    else if (type instanceof BooleanType) ps.setBoolean(i+1,row.getFieldAs(i));
                    else if (type instanceof UuidType) ps.setString(i+1,row.getFieldAs(i));
                    else if (type instanceof DateTimeType) ps.setTimestamp(i+1, Timestamp.from(row.getFieldAs(i)));
                    else throw new UnsupportedOperationException("Unexpected datatype:" + type);
                }
            }
        }
    }

    private synchronized java.sql.Connection getConnection() throws Exception {
        if (connectionProvider == null) {
            connectionProvider = new SimpleJdbcConnectionProvider(options);
        }
        return connectionProvider.getOrEstablishConnection();
    }

    private void createTable(String tableName, DestinationTableSchema schema) throws Exception {
        DSLContext context = DSL.using(getConnection(), dialect);
        CreateTableColumnStep createTable = context.createTableIfNotExists(tableName);
        for (DestinationTableSchema.Field field : schema) {
            createTable = createTable.column(field.getName(), getJooqSQLType(field));
        }
        CreateTableConstraintStep tableConstraint = createTable.primaryKey(getPrimaryKeyNames(schema).toArray(new String[0]));
        tableConstraint.execute();
    }

    private static List<String> getPrimaryKeyNames(DestinationTableSchema schema) {
        List<String> primaryKeys = new ArrayList<>();
        for (DestinationTableSchema.Field field : schema) {
            if (field.isPrimaryKey()) primaryKeys.add(field.getName());
        }
        return primaryKeys;
    }

    public static DataType<?> getJooqSQLType(DestinationTableSchema.Field f) {
        ScalarType type = f.getType();
        DataType mapType;
        if (f.getType() instanceof StringType) mapType = SQLDataType.LONGVARCHAR;
        else if (f.getType() instanceof IntegerType) mapType = SQLDataType.BIGINT;
        else if (f.getType() instanceof FloatType) mapType = SQLDataType.DOUBLE;
        else if (f.getType() instanceof NumberType) mapType = SQLDataType.DOUBLE;
        else if (f.getType() instanceof BooleanType) mapType = SQLDataType.BOOLEAN;
        else if (f.getType() instanceof UuidType) mapType = SQLDataType.VARCHAR(36);
        else if (f.getType() instanceof DateTimeType) mapType = SQLDataType.INSTANT;
        else throw new UnsupportedOperationException("Unexpected datatype:" + type);

        if (f.isArray()) mapType = mapType.getArrayDataType();
        else if (f.isNonNull()) mapType = mapType.notNull();

        return mapType;
    }

}
