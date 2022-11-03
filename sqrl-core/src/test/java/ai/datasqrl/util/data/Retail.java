package ai.datasqrl.util.data;

import ai.datasqrl.util.ScriptBuilder;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.TestScript;

import java.nio.file.Path;

public class Retail implements TestDataset, TestScript {

    public static final Path BASE_PATH = Path.of("..","sqml-examples","retail");

    public static final Retail INSTANCE = new Retail();

    @Override
    public String getName() {
        return "ecommerce-data";
    }

    @Override
    public Path getDataDirectory() {
        return BASE_PATH.resolve("data");
    }

    @Override
    public int getNumTables() {
        return 3;
    }

    @Override
    public Path getRootPackageDirectory() {
        return BASE_PATH;
    }

    @Override
    public Path getScript() {
        return BASE_PATH.resolve(Path.of("c360","c360.sqrl"));
    }

    public ScriptBuilder getImports() {
        ScriptBuilder builder = new ScriptBuilder();
        builder.append("IMPORT ecommerce-data.Customer");
        builder.append("IMPORT ecommerce-data.Orders");
        builder.append("IMPORT ecommerce-data.Product");
        return builder;
    }
}
