package ai.datasqrl.util.data;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.util.ScriptComplexity;
import ai.datasqrl.util.TestDataset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.SneakyThrows;
import org.checkerframework.checker.units.qual.C;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class C360 implements TestDataset {

    public static final C360 INSTANCE = new C360();

    public static final Path RETAIL_DIR = Path.of("../sqml-examples/retail/");
    public static final String RETAIL_SCRIPT_NAME = "c360";
    public static final Path RETAIL_SCRIPT_DIR = RETAIL_DIR.resolve(RETAIL_SCRIPT_NAME);
    public static final Path RETAIL_IMPORT_SCHEMA_FILE = RETAIL_SCRIPT_DIR.resolve("pre-schema.yml");
    public static final String RETAIL_DATA_DIR_NAME = "ecommerce-data";
    public static final String RETAIL_DATASET = "ecommerce-data";
    public static final Path RETAIL_DATA_DIR = RETAIL_DIR.resolve(RETAIL_DATA_DIR_NAME);

    @Override
    public String getName() {
        return RETAIL_DATASET;
    }

    @Override
    public DirectorySourceImplementation getSource() {
        return DirectorySourceImplementation.builder()
                .uri(RETAIL_DATA_DIR.toAbsolutePath().toString())
                .build();
    }

    @Override
    public Map<String, Integer> getTableCounts() {
        return ImmutableMap.of("orders",4, "customer",4, "product",6);
    }

    @Override
    public String getScriptContent(ScriptComplexity complexity) {
        return "IMPORT ecommerce-data.Orders;";
    }

    @Override
    @SneakyThrows
    public Optional<String> getInputSchema() {
        return Optional.of(Files.readString(C360.RETAIL_IMPORT_SCHEMA_FILE));
    }

}
