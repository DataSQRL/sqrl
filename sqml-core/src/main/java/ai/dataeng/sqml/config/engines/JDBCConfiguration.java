package ai.dataeng.sqml.config.engines;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class JDBCConfiguration implements EngineConfiguration.Database {

    @NotNull
    public String dbURL;
    @Nullable
    public String user;
    @Nullable
    public String password;
    @Nullable
    public String driverName;

    @NotNull
    public Dialect dialect;

    public enum Dialect {

        POSTGRES, MYSQL, H2;

    }

}
