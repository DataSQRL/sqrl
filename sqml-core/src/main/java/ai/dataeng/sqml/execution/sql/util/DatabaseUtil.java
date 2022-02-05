package ai.dataeng.sqml.execution.sql.util;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.execution.sql.DatabaseSink;
import ai.dataeng.sqml.execution.sql.SQLConfiguration;
import ai.dataeng.sqml.execution.sql.SQLJDBCQueryBuilder;
import ai.dataeng.sqml.type.basic.*;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.NotImplementedException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
public class DatabaseUtil {

    @NonNull
    private final SQLConfiguration configuration;


    @Value
    public static class SQLTypeMapping {

        private final String typeName;
        private final int typeNo;

    }

    private String makeArrayDataType(String dataType) {
        switch(configuration.getDialect()) {
            case POSTGRES:
                return dataType + "[]";
            case H2:
                return "array";
            default:
                return dataType + "[]";
        }
    }

    public SQLTypeMapping getSQLType(Column column) {
        BasicType type = column.getType();
        SQLTypeMapping mapType;
        if (type instanceof StringType) mapType = new SQLTypeMapping("VARCHAR",12);
        else if (type instanceof IntegerType) mapType = new SQLTypeMapping("BIGINT",-5);
        else if (type instanceof FloatType) mapType = new SQLTypeMapping("DOUBLE",-5);
        else if (type instanceof NumberType) mapType = new SQLTypeMapping("DOUBLE",8);
        else if (type instanceof BooleanType) mapType = new SQLTypeMapping("BIT",16);
        else if (type instanceof UuidType) mapType = new SQLTypeMapping("CHAR(36)",12);
        else if (type instanceof DateTimeType) mapType = new SQLTypeMapping("TIMESTAMP WITH TIME ZONE",12);
        else throw new UnsupportedOperationException("Unexpected datatype:" + type);

        if (column.getArrayDepth() > 0) {
            mapType = new SQLTypeMapping(makeArrayDataType(mapType.getTypeName()),2003);
        }
        return mapType;
    }


    public static final String TIMESTAMP_COLUMN_NAME = "__sqrl_timestamp";
    public static final SQLTypeMapping TIMESTAMP_COLUMN_SQL_TYPE = new SQLTypeMapping("TIMESTAMP WITH TIME ZONE",12);


    public DatabaseSink getSink(String tableName, Column[] schema) {
        //1) Upsert
        StringBuilder s = new StringBuilder();
        if (configuration.getDialect() == SQLConfiguration.Dialect.H2) {
            s.append("MERGE INTO ").append(sqlName(tableName)).append(" KEY (");
            s.append(getPrimaryKeyNames(schema).map(n -> sqlName(n)).collect(Collectors.joining(", ")));
            s.append(") VALUES (");
            s.append(Arrays.stream(new int[schema.length + 1]).mapToObj(i -> "?").collect(Collectors.joining(", ")));
            s.append(");");
        } else if (configuration.getDialect() == SQLConfiguration.Dialect.POSTGRES) {
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

    private static Stream<String> getPrimaryKeyNames(Column[] schema) {
        return Arrays.stream(schema).filter(Column::isPrimaryKey).map(
            Column::getId);
    }

    private LinkedHashMap<Integer, Integer> getPosition2Type(Column[] schema, boolean primaryKeysOnly) {
        LinkedHashMap<Integer, Integer> pos2type = new LinkedHashMap<>(schema.length);
        int pos = 0;
        for(Column col :schema) {
            if (!primaryKeysOnly || col.isPrimaryKey()) {
                pos2type.put(pos,getSQLType(col).getTypeNo());
            }
            pos++;
        }
        return pos2type;
    }

    public static String result2String(ResultSet result) throws SQLException {
        ResultSetMetaData rsmd = result.getMetaData();
        int colNo = rsmd.getColumnCount();
        StringBuilder s = new StringBuilder();
        for (int i = 1; i <= colNo; i++) {
            s.append(rsmd.getColumnName(i));
            s.append(" | ");
        }
        while (result.next()) {
            s.append("\n");
            for (int i = 1; i <= colNo; i++) {
                s.append(result.getString(i));
                s.append(" | ");
            }
        }
        return s.toString();
    }





}
