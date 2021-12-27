package ai.dataeng.sqml.execution.sql;

import ai.dataeng.sqml.execution.flink.process.Row;
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
