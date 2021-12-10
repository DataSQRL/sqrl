package ai.dataeng.sqml.physical.sql;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.physical.DatabaseSink;
import ai.dataeng.sqml.physical.flink.Row;
import ai.dataeng.sqml.physical.flink.RowUpdate;
import ai.dataeng.sqml.schema2.basic.*;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@AllArgsConstructor
public class SQLJDBCQueryBuilder implements DatabaseSink.JDBCQueryBuilder {

    private LinkedHashMap<Integer,Integer> pos2SQLType;
    private String query;

    private SQLJDBCQueryBuilder() {} //Kryo

    @Override
    public String getSQLQuery() {
        return query;
    }

    @Override
    public void setArguments(PreparedStatement ps, Row row, Instant ingestTime) throws SQLException {
        int queryPos = 1;
        for (Map.Entry<Integer,Integer> col : pos2SQLType.entrySet()) {
            int position = col.getKey();
            int sqlType = col.getValue();
            Object value = row.getValue(position);
            if (value == null) ps.setNull(queryPos, sqlType);
//            else if (field.isArray()) ps.setArray(queryPos, (Array) value); //TODO: need to properly wrap in java.sql.Array
            else ps.setObject(queryPos, value, sqlType);
            queryPos++;
        }
        ps.setObject(queryPos, ingestTime, DatabaseUtil.TIMESTAMP_COLUMN_JOOQ_TYPE.getSQLType());
    }
}
