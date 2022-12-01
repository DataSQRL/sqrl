package ai.datasqrl.compile;

import ai.datasqrl.config.GlobalCompilerConfiguration;
import ai.datasqrl.config.GlobalEngineConfiguration;

import java.nio.file.Path;

public class CompilerConfigInitializer {

    public static GlobalCompilerConfiguration readConfig(Path path) {
        GlobalCompilerConfiguration config = GlobalEngineConfiguration.readFrom(path, GlobalCompilerConfiguration.class);
        //TODO: set default engines - how do we initialize database?
//        JDBCTestDatabase jdbcDB = new JDBCTestDatabase(getDatabase());
//        config.setDefaultEngines(jdbcDB.getJdbcConfiguration(),new FlinkEngineConfiguration());
        return config;
    }

}
