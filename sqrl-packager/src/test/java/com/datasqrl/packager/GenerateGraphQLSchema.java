package com.datasqrl.packager;

import com.datasqrl.graphql.generate.SchemaGeneratorUseCaseTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class GenerateGraphQLSchema extends SchemaGeneratorUseCaseTest {

    @Test
    public void writeSchemaFile() {
        produceSchemaFile(DataSQRL.INSTANCE.getScript());
    }


}
