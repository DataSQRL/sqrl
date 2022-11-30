package ai.datasqrl.compiler;

import ai.datasqrl.AbstractEngineIT;
import ai.datasqrl.IntegrationTestSettings;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigurationIT extends AbstractEngineIT {

    @Test
    public void testSettings() {
        initialize(IntegrationTestSettings.getInMemory());
        assertNotNull(database);
        assertNotNull(engineSettings);
    }


}
