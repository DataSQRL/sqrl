package ai.datasqrl.packager;

import ai.datasqrl.graphql.generate.SchemaGeneratorUseCaseTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GenerateGraphQLSchema extends SchemaGeneratorUseCaseTest {

    @Test
    @Disabled
    public void writeSchemaFile() {
        produceSchemaFile(DataSQRL.INSTANCE.getScript());
    }


}
