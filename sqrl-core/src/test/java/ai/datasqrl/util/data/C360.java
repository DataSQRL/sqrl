package ai.datasqrl.util.data;

import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
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

    public static final Path RETAIL_DIR_BASE = Path.of("../sqml-examples/retail/");

    public static final String RETAIL_SCRIPT_NAME_BASE = "c360";
    public static final String RETAIL_DATASET_BASE = "ecommerce-data";


    private final String scriptName;
    private final Path scriptDir;
    private final Path importSchema;
    private final Path discoveredSchema;
    private final String datasetName;
    private final Path dataDir;

    private C360(String version) {
        this.scriptName = RETAIL_SCRIPT_NAME_BASE + version;
        this.scriptDir = RETAIL_DIR_BASE.resolve(scriptName);
        this.importSchema = scriptDir.resolve("pre-schema.yml");
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
    public DirectorySourceImplementation getSource() {
        return DirectorySourceImplementation.builder()
                .uri(dataDir.toAbsolutePath().toString())
                .build();
    }

    @Override
    public Map<String, Integer> getTableCounts() {
        return ImmutableMap.of("orders",4, "customer",4, "product",6, "entries", 7);
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
        return Optional.of(Files.readString(importSchema));
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
