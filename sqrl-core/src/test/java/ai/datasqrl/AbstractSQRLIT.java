package ai.datasqrl;

import ai.datasqrl.compile.loaders.DataSourceLoader;
import ai.datasqrl.config.DiscoveryConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Session;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.junit.jupiter.api.AfterEach;

import java.nio.file.Files;
import java.nio.file.Path;

public class AbstractSQRLIT extends AbstractEngineIT {

    @AfterEach
    public void tearDown() {
        super.tearDown();
        error = null;

    }

    public ErrorCollector error;
    public ConfiguredSqrlParser parser;
    public Planner planner;
    public Resolve resolve;
    public Session session;
    public Path rootDir;
    public JDBCConnectionProvider jdbc;
    public PhysicalPlanner physicalPlanner;


    protected void initialize(IntegrationTestSettings settings, Path rootDir) {
        super.initialize(settings);
        error = ErrorCollector.root();

        planner = new PlannerFactory(
                new SqrlCalciteSchema(CalciteSchema.createRootSchema(false, false).plus()).plus()).createPlanner();
        Session session = new Session(error, planner);
        this.session = session;
        this.parser = new ConfiguredSqrlParser(error);
        this.resolve = new Resolve(rootDir);
        this.rootDir = rootDir;

        DatabaseConnectionProvider db = sqrlSettings.getDatabaseEngineProvider().getDatabase(DiscoveryConfiguration.MetaData.DEFAULT_DATABASE);
        jdbc = (JDBCConnectionProvider) db;

        physicalPlanner = new PhysicalPlanner(jdbc,
                sqrlSettings.getStreamEngineProvider().create(), planner);
    }

    protected TableSource loadTable(NamePath path) {
        DataSourceLoader loader = new DataSourceLoader();
        return loader.readTable(rootDir,path,error).get();
    }

    @SneakyThrows
    protected String loadScript(String name) {
        Path path = rootDir.resolve(name);
        return Files.readString(path);
    }

}
