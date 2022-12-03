package ai.datasqrl.packager;

import ai.datasqrl.io.TestDataSetMonitoringIT;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class GenerateInputDataSchema extends TestDataSetMonitoringIT {

    @Test
    public void generateSchema() {
        generateTableConfigAndSchemaInDataDir(DataSQRL.INSTANCE);
    }


}
