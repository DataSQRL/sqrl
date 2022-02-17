package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import lombok.NonNull;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public interface JDBCConnectionProvider extends Serializable {

    @NonNull String getDbURL();

    String getUser();

    String getPassword();

    String getDriverName();

    @NonNull String getDatabaseName();

    @NotNull JDBCConfiguration.Dialect getDialect();

    public Connection getConnection() throws SQLException, ClassNotFoundException;

}
