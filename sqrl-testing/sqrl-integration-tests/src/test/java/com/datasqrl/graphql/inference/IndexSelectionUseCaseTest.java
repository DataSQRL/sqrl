/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.TestScript.QueryUseCaseProvider;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Retail.RetailScriptNames;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class IndexSelectionUseCaseTest extends AbstractSchemaInferenceModelTest {

  @ParameterizedTest
  @ArgumentsSource(QueryUseCaseProvider.class)
  public void fullScriptTest(TestScript script, TestGraphQLSchema graphQLSchema) {
    SnapshotTest.Snapshot snapshot = SnapshotTest.Snapshot.of(getClass(), script.getName(),
        graphQLSchema.getName());
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory(), Optional.empty());

    String result = selectIndexes(script, graphQLSchema.getSchemaPath()).entrySet().stream()
        .map(e -> String.format("%s - %.3f",e.getKey().getName(),e.getValue())).sorted()
        .collect(Collectors.joining(System.lineSeparator()));
    snapshot.addContent(result);
    snapshot.createOrValidate();
  }

  @Test
  @Disabled
  public void testSingle() {
    TestScript script = Retail.INSTANCE.getScript(RetailScriptNames.SEARCH);
    fullScriptTest(script, script.getGraphQLSchemas().get(0));
  }


}
