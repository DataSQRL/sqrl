/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.util.SnapshotTest.Snapshot;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class AbstractGraphqlSchemaFactoryTest extends AbstractLogicalSQRLIT {

  private Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = Snapshot.of(getClass(), testInfo);
  }

  protected String generateSchema(String sqrlScript, boolean addArguments) {
    plan(sqrlScript);

    GraphQLSchema schema = new GraphqlSchemaFactory(injector.getInstance(SqrlSchema.class), addArguments)
        .generate();
    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);

    return new SchemaPrinter(opts).print(schema);
  }

  protected void snapshotTest(String sqrlScript) {
    snapshotTest(sqrlScript, true);
  }

  protected void snapshotTest(String sqrlScript, boolean addArguments) {
    snapshot.addContent(generateSchema(sqrlScript, addArguments));
    snapshot.createOrValidate();
  }
}