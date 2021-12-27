package ai.dataeng.sqml.execution.sql;

import lombok.Value;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Value
public class SQLConfiguration {

    private final Dialect dialect;

    private final JdbcConnectionOptions jdbcConnectionOptions;

    public Connection getConnection() throws SQLException, ClassNotFoundException {
        return DriverManager.getConnection(jdbcConnectionOptions.getDbURL(),
                    jdbcConnectionOptions.getUsername().orElse(null),
                    jdbcConnectionOptions.getPassword().orElse(null));

    }

    public enum Dialect {

        POSTGRES, MYSQL, H2;

    }

}
