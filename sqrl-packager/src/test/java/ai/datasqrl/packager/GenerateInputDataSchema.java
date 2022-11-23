package ai.datasqrl.packager;

import ai.datasqrl.io.TestDataSetMonitoringIT;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GenerateInputDataSchema extends TestDataSetMonitoringIT {

    @Test
    @Disabled
    public void generateSchema() {
        generateTableConfigAndSchemaInDataDir(DataSQRL.INSTANCE);
    }


}
