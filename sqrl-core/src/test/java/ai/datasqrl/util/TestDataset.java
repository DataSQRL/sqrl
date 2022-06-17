package ai.datasqrl.util;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.Environment;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;

public interface TestDataset {

    String getName();

    DirectorySourceImplementation getSource();

    default void registerSource(Environment env) {
        ErrorCollector errors = ErrorCollector.root();
        env.getDatasetRegistry().addOrUpdateSource(getName(), getSource(), errors);
        assertFalse(errors.isFatal(),errors.toString());
    }

    Map<String, Integer> getTableCounts();

    String getScriptContent(ScriptComplexity complexity);

    public Optional<String> getInputSchema();

    default TestScriptBundleBuilder buildBundle() {
        return new TestScriptBundleBuilder(this);
    }

}
