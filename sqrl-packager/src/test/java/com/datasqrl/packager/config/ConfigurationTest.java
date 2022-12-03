package ai.datasqrl.packager.config;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class ConfigurationTest {

    public static final Path RESOURCE_DIR = Paths.get("src","test","resources");

    @Test
    @SneakyThrows
    public void testConfiguration() {
        GlobalPackageConfiguration config = GlobalPackageConfiguration.readFrom(RESOURCE_DIR.resolve("package1.json"));
        assertNotNull(config);
        assertEquals("1.0.0",config.getPkg().getVersion());
        assertEquals(2, config.getDependencies().size());
        assertEquals(config.getDependencies().get("datasqrl.examples.Basic").getVersion(),"1.0.0");
        assertEquals(config.getDependencies().get("datasqrl.examples.Shared").getVariant(),"dev");
    }

}
