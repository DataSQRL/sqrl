package com.datasqrl;

import com.datasqrl.compile.loaders.DataSource;
import com.datasqrl.config.error.ErrorCollector;
import com.datasqrl.io.sources.dataset.TableSource;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.parse.tree.name.NamePath;
import com.datasqrl.plan.calcite.Planner;
import com.datasqrl.plan.calcite.PlannerFactory;
import com.datasqrl.plan.local.generate.Resolve;
import com.datasqrl.plan.local.generate.Session;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.junit.jupiter.api.AfterEach;

import java.nio.file.Files;
import java.nio.file.Path;

public class AbstractLogicalSQRLIT extends AbstractEngineIT {

    @AfterEach
    public void tearDown() {
        super.tearDown();
        error = null;

    }

    public ErrorCollector error;
    public SqrlParser parser;
    public Planner planner;
    public Resolve resolve;
    public Session session;
    public Path rootDir;


    protected void initialize(IntegrationTestSettings settings, Path rootDir) {
        super.initialize(settings);
        error = ErrorCollector.root();

        planner = new PlannerFactory(
                new SqrlCalciteSchema(CalciteSchema.createRootSchema(false, false).plus()).plus()).createPlanner();
        Session session = new Session(error, planner, engineSettings.getPipeline());
        this.session = session;
        this.parser = new SqrlParser();
        this.resolve = new Resolve(rootDir);
        this.rootDir = rootDir;
    }

    protected TableSource loadTable(NamePath path) {
        DataSource.Loader loader = new DataSource.Loader();
        return loader.readTable(rootDir,path,error).get();
    }

    @SneakyThrows
    protected String loadScript(String name) {
        Path path = rootDir.resolve(name);
        return Files.readString(path);
    }

}
