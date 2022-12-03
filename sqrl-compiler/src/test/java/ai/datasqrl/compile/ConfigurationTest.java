package ai.datasqrl.compile;

import ai.datasqrl.config.EngineSettings;
import ai.datasqrl.config.GlobalCompilerConfiguration;
import ai.datasqrl.config.GlobalEngineConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigurationTest {

    public static final Path RESOURCE_DIR = Paths.get("src","test","resources");

    @Test
    public void testConfiguration() {
        GlobalCompilerConfiguration config = GlobalEngineConfiguration.readFrom(RESOURCE_DIR.resolve("package1.json"), GlobalCompilerConfiguration.class);
        assertNotNull(config);
        assertEquals(3, config.getCompiler().getApi().getMaxArguments());
        assertEquals(2, config.getEngines().size());
        ErrorCollector errors = ErrorCollector.root();
        EngineSettings engineSettings = config.initializeEngines(errors);
        assertNotNull(engineSettings, errors.toString());
        assertNotNull(engineSettings.getJDBC());
    }

}
