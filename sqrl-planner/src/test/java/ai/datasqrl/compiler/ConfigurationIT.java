package ai.datasqrl.compiler;

import ai.datasqrl.AbstractEngineIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.GlobalConfiguration;
import ai.datasqrl.util.TestResources;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigurationIT extends AbstractEngineIT {

    @Test
    @Disabled
    public void testConfigFromFile() {
        GlobalConfiguration config = GlobalConfiguration.fromFile(TestResources.CONFIG_YML);
        IntegrationTestSettings.validateConfig(config);
        assertEquals(config.getEngines().getJdbc().getDbURL(),"jdbc:h2:tmp/output");
        assertNotNull(config.getEngines().getFlink());
        assertEquals(config.getDiscovery().getMetastore().getDatabaseName(),"system");
    }

    @Test
    public void testSettings() {
        initialize(IntegrationTestSettings.getInMemory());
        assertNotNull(database);
        assertNotNull(sqrlSettings);
    }


}
