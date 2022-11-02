package ai.datasqrl.util.data;

import ai.datasqrl.util.ScriptBuilder;
import ai.datasqrl.util.TestDataset;

import java.nio.file.Path;
import java.util.List;

public class Retail implements TestDataset {

    public static final Path BASE_PATH = Path.of("..","sqml-examples","retail");

    public static final Retail INSTANCE = new Retail();

    @Override
    public String getName() {
        return "retail";
    }

    @Override
    public Path getDataDirectory() {
        return BASE_PATH.resolve("ecommerce-data");
    }

    @Override
    public Path getRootPackageDirectory() {
        return BASE_PATH;
    }

    @Override
    public ScriptBuilder getImports() {
        ScriptBuilder builder = new ScriptBuilder();
        builder.append("IMPORT ecommerce-data.Customer");
        builder.append("IMPORT ecommerce-data.Orders");
        builder.append("IMPORT ecommerce-data.Product");
        return builder;
    }

    @Override
    public List<Path> getScripts() {
        return List.of(BASE_PATH.resolve(Path.of("c360","c360.sqrl")));
    }
}
