package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.physical.flink.Row;
import ai.dataeng.sqml.physical.flink.RowUpdate;
import lombok.Data;
import lombok.Value;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

@Value
public class DatabaseSink {

    private final QueryBuilder upsertQuery;
    private final QueryBuilder deleteQuery;

    public interface QueryBuilder extends Serializable {

    }

    public interface JDBCQueryBuilder extends QueryBuilder {

        public String getSQLQuery();

        public void setArguments(PreparedStatement ps, Row row, Instant ingestTime) throws SQLException;

    }

}
