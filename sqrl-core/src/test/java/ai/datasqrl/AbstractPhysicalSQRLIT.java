package ai.datasqrl;

import ai.datasqrl.config.DiscoveryConfiguration;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.physical.PhysicalPlanner;

import java.nio.file.Path;

public class AbstractPhysicalSQRLIT extends AbstractLogicalSQRLIT {

    public JDBCConnectionProvider jdbc;
    public PhysicalPlanner physicalPlanner;

    protected void initialize(IntegrationTestSettings settings, Path rootDir) {
        super.initialize(settings, rootDir);

        DatabaseConnectionProvider db = sqrlSettings.getDatabaseEngineProvider().getDatabase(DiscoveryConfiguration.MetaData.DEFAULT_DATABASE);
        jdbc = (JDBCConnectionProvider) db;

        physicalPlanner = new PhysicalPlanner(jdbc,
                sqrlSettings.getStreamEngineProvider().create(), planner);
    }


}
