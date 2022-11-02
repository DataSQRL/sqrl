package ai.datasqrl.util.data;

import ai.datasqrl.io.impl.file.DirectoryDataSystem;
import ai.datasqrl.util.ScriptBuilder;
import ai.datasqrl.util.ScriptComplexity;
import ai.datasqrl.util.TestDataset;
import com.google.common.collect.ImmutableMap;
import lombok.SneakyThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public class C360 implements TestDataset {

    public static final Path RETAIL_DIR_BASE = Path.of("../sqml-examples/retail-example-bundle/");

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

    @Override
    public String getName() {
        return datasetName;
    }

    @Override
    public DirectoryDataSystem getSource() {
        return DirectoryDataSystem.builder()
                .uri(dataDir.toAbsolutePath().toString())
                .build();
    }

    @Override
    public Map<String, Integer> getTableCounts() {
        return ImmutableMap.of("orders",4, "customer",5, "product",6);
    }

    @Override
    public String getScriptContent(ScriptComplexity complexity) {
        return "IMPORT "+datasetName+".Orders;";
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
    @SneakyThrows
    public Optional<String> getInputSchema() {
        return Optional.empty();
    }

    @Override
    @SneakyThrows
    public Optional<String> getDiscoveredSchema() {
        return Optional.of(Files.readString(discoveredSchema));
    }

    public static final C360 BASIC = new C360("");
    /* Multiple categories per product as nested entity */
    public static final C360 NESTED_CATEGORIES = new C360("v2");

}
