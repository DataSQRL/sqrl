package ai.dataeng.sqml.physical.sql;

import lombok.Value;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.sql.Connection;
import java.sql.SQLException;

@Value
public class SQLConfiguration {

    private final SQLDialect dialect;

    private final JdbcConnectionOptions jdbcConnectionOptions;

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        SimpleJdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(jdbcConnectionOptions);
        return connectionProvider.getOrEstablishConnection();
    }

    public DSLContext getJooQ() throws SQLException, ClassNotFoundException {
        return DSL.using(getConnection(), dialect);
    }

}
