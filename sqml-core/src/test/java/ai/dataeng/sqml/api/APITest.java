package ai.dataeng.sqml.api;

import ai.dataeng.sqml.config.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class APITest {

    Path resourceDir = Paths.get("src","test","resources");
    Path configYml = resourceDir.resolve("simple-config.yml");

    @Test
    public void testConfig() {
        GlobalConfiguration config = GlobalConfiguration.fromFile(configYml);
        System.out.println("URL: "+config.getEngines().getJdbc().getDbURL());
    }

}
