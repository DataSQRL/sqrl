package ai.datasqrl.config.engines;

import ai.datasqrl.config.constraints.OptionalMinString;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JDBCConfiguration implements EngineConfiguration.Database {

    @NonNull @NotNull
    String dbURL;
    String user;
    String password;
    @OptionalMinString
    String driverName;
    @NonNull @NotNull
    Dialect dialect;

    public enum Dialect {

        POSTGRES, MYSQL, H2;

    }

    public static Pattern validDBName = Pattern.compile("^[a-z][_a-z0-9]{2,}$");

    public Database getDatabase(@NonNull String databaseName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(databaseName)
                && validDBName.matcher(databaseName).matches(),"Invalid database name: %s", databaseName);
        //Construct URL pointing at database
        String url = dbURL;
        switch (dialect) {
            case H2:
            case MYSQL:
            case POSTGRES:
                if (!url.endsWith("/")) url+="/";
                url += databaseName;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported dialect: " + dialect);
        }

        //Modify url for database engine
        if (dialect.equals(Dialect.H2)) {
            url += ";database_to_upper=false";
        }

        return new Database(url, user, password, driverName, dialect, databaseName);
    }

    @Getter
    @AllArgsConstructor
    @EqualsAndHashCode
    @ToString
    public static class Database implements JDBCConnectionProvider {

        @NonNull
        private String dbURL;
        private String user;
        private String password;
        private String driverName;
        private Dialect dialect;
        private String databaseName;

        @Override
        public Connection getConnection() throws SQLException, ClassNotFoundException {
            return DriverManager.getConnection(dbURL, user, password);
        }
    }

}
