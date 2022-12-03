package ai.datasqrl.util.data;

import ai.datasqrl.util.ScriptBuilder;

import java.nio.file.Path;

public class C360 {

    public static final Path RETAIL_DIR_BASE = Path.of("../sqrl-examples/retail-example-bundle/");

    public static final String RETAIL_SCRIPT_NAME_BASE = "c360";
    public static final String RETAIL_DATASET_BASE = "ecommerce-data";


    private final String scriptName;
//    private final Path importSchema;
    private final Path discoveredSchema;
    private final String datasetName;
    private final Path dataDir;

    private C360(String version) {
        this.scriptName = RETAIL_SCRIPT_NAME_BASE + version;
//        this.importSchema = RETAIL_DIR_BASE.resolve("pre-schema.yml");
        this.datasetName = RETAIL_DATASET_BASE + version;
        this.dataDir = RETAIL_DIR_BASE.resolve(datasetName);
        this.discoveredSchema = dataDir.resolve("discovered-schema.yml");
    }

    @Override
    public String toString() {
        return scriptName;
    }

    public String getName() {
        return datasetName;
    }


    public ScriptBuilder getImports() {
        ScriptBuilder builder = new ScriptBuilder();
        builder.append("IMPORT ecommerce-data.Customer");
        builder.append("IMPORT ecommerce-data.Orders");
        builder.append("IMPORT ecommerce-data.Product");
        return builder;
    }


    public static final C360 BASIC = new C360("");
    /* Multiple categories per product as nested entity */
    public static final C360 NESTED_CATEGORIES = new C360("v2");

}
