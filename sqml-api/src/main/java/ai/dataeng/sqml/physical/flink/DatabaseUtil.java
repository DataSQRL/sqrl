package ai.dataeng.sqml.physical.flink;

import ai.dataeng.sqml.logical4.StreamType;
import ai.dataeng.sqml.physical.DatabaseSink;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.EnumSet;

public class DatabaseUtil {

    private final FlinkConfiguration configuration;

    DatabaseUtil(FlinkConfiguration configuration) {
        this.configuration = configuration;
    }

    SinkFunction<RowUpdate> getDatabaseSink(DatabaseSink.QueryBuilder queryBuilder, StreamType whichRow) {
        if (queryBuilder instanceof DatabaseSink.JDBCQueryBuilder) {
            return getJDBCDatabaseSink((DatabaseSink.JDBCQueryBuilder) queryBuilder, whichRow);
        } else throw new UnsupportedOperationException("Unsupported query builder: " + queryBuilder);
    }

    SinkFunction<RowUpdate> getJDBCDatabaseSink(DatabaseSink.JDBCQueryBuilder queryBuilder, StreamType whichRow) {
        return JdbcSink.sink(queryBuilder.getSQLQuery(), new StatementBuilderWrapper(queryBuilder, whichRow),
                JdbcExecutionOptions.defaults(), configuration.getJdbcConnectionOptions());
    }

    @AllArgsConstructor
    static class StatementBuilderWrapper implements Serializable, JdbcStatementBuilder<RowUpdate> {

        DatabaseSink.JDBCQueryBuilder queryBuilder;
        StreamType whichRow;

        private StatementBuilderWrapper() {} //Kryo

        @Override
        public void accept(PreparedStatement preparedStatement, RowUpdate row) throws SQLException {
            Row r = (whichRow==StreamType.APPEND)?row.getAppend():row.getRetraction();
            queryBuilder.setArguments(preparedStatement, r, row.getIngestTime());
        }
    }

    public static class RowUpdateFilter implements FilterFunction<RowUpdate> {

        private EnumSet<RowUpdate.Type> acceptedTypes;

        private RowUpdateFilter() {} //Kryo

        public RowUpdateFilter(EnumSet<RowUpdate.Type> acceptedTypes) {
            this.acceptedTypes = acceptedTypes;
        }

        public RowUpdateFilter(RowUpdate.Type first, RowUpdate.Type... rest) {
            this(EnumSet.of(first,rest));
        }

        @Override
        public boolean filter(RowUpdate rowUpdate) throws Exception {
            return acceptedTypes.contains(rowUpdate.getType());
        }
    }

}
