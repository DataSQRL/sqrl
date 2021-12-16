package ai.dataeng.sqml.physical.sql;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.db.tabular.JDBCSinkFactory;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.physical.DatabaseSink;
import ai.dataeng.sqml.schema2.basic.*;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.NotImplementedException;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
public class DatabaseUtil {

    private final SQLConfiguration configuration;

    public static DataType<?> getJooqSQLType(LogicalPlan.Column column) {
        BasicType type = column.getType();
        DataType mapType;
        if (type instanceof StringType) mapType = SQLDataType.LONGVARCHAR;
        else if (type instanceof IntegerType) mapType = SQLDataType.BIGINT;
        else if (type instanceof FloatType) mapType = SQLDataType.DOUBLE;
        else if (type instanceof NumberType) mapType = SQLDataType.DOUBLE;
        else if (type instanceof BooleanType) mapType = SQLDataType.BOOLEAN;
        else if (type instanceof UuidType) mapType = SQLDataType.VARCHAR(36);
        else if (type instanceof DateTimeType) mapType = SQLDataType.INSTANT;
        else throw new UnsupportedOperationException("Unexpected datatype:" + type);

        if (column.getArrayDepth() > 0) mapType = mapType.getArrayDataType();
        else if (column.isNonNull()) mapType = mapType.notNull();
        return mapType;
    }


    public static final String TIMESTAMP_COLUMN_NAME = "__sqrl_timestamp";
    public static final BasicType TIMESTAMP_COLUMN_TYPE = DateTimeType.INSTANCE;
    public static final DataType<?> TIMESTAMP_COLUMN_JOOQ_TYPE = SQLDataType.INSTANT.notNull();

    public String createTableDML(String tableName, LogicalPlan.Column[] schema) {
        DSLContext context = DSL.using(configuration.getDialect());
        CreateTableColumnStep createTable = context.createTableIfNotExists(tableName);
        for (LogicalPlan.Column column : schema) {
            createTable = createTable.column(column.getId(), getJooqSQLType(column));
        }
        createTable = createTable.column(TIMESTAMP_COLUMN_NAME, TIMESTAMP_COLUMN_JOOQ_TYPE);
        CreateTableConstraintStep tableConstraint = createTable.primaryKey(getPrimaryKeyNames(schema).toArray(String[]::new));
        return tableConstraint.getSQL();
    }

    public DatabaseSink getSink(String tableName, LogicalPlan.Column[] schema) {
        //1) Upsert
        StringBuilder s = new StringBuilder();
        if (configuration.getDialect() == SQLDialect.H2) {
            s.append("MERGE INTO ").append(sqlName(tableName)).append(" KEY (");
            s.append(getPrimaryKeyNames(schema).map(n -> sqlName(n)).collect(Collectors.joining(", ")));
            s.append(") VALUES (");
            s.append(Arrays.stream(new int[schema.length + 1]).mapToObj(i -> "?").collect(Collectors.joining(", ")));
            s.append(");");
        } else if (configuration.getDialect() == SQLDialect.POSTGRES) {
            throw new NotImplementedException("Not yet implemented");
        } else throw new UnsupportedOperationException();
        String upsertQuery = s.toString();
        LinkedHashMap<Integer, Integer> upsertPos2type = getPosition2Type(schema,false);
        System.out.println(upsertQuery + " --> " + upsertPos2type);

        //2) Delete
        s = new StringBuilder();
        s.append("DELETE FROM ").append(sqlName(tableName)).append(" WHERE ");
        s.append(getPrimaryKeyNames(schema).map(n -> sqlName(n) + "= ?").collect(Collectors.joining(" AND ")));
        s.append(" AND ").append(sqlName(TIMESTAMP_COLUMN_NAME)).append(" < ?");
        String deleteQuery = s.toString();
        LinkedHashMap<Integer, Integer> deletePos2type = getPosition2Type(schema,true);
        System.out.println(deleteQuery + " --> " + deletePos2type);

        return new DatabaseSink(new SQLJDBCQueryBuilder(upsertPos2type, upsertQuery),
                new SQLJDBCQueryBuilder(deletePos2type, deleteQuery));
    }

    public static final String sqlName(String name) {
        return "\"" + name + "\"";
    }

    private static Stream<String> getPrimaryKeyNames(LogicalPlan.Column[] schema) {
        return Arrays.stream(schema).filter(LogicalPlan.Column::isPrimaryKey).map(LogicalPlan.Column::getId);
    }

    private static LinkedHashMap<Integer, Integer> getPosition2Type(LogicalPlan.Column[] schema, boolean primaryKeysOnly) {
        LinkedHashMap<Integer, Integer> pos2type = new LinkedHashMap<>(schema.length);
        int pos = 0;
        for(LogicalPlan.Column col :schema) {
            if (!primaryKeysOnly || col.isPrimaryKey()) {
                pos2type.put(pos,getJooqSQLType(col).getSQLType());
            }
            pos++;
        }
        return pos2type;
    }





}
