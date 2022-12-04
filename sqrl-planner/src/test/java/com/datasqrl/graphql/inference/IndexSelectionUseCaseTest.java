/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestGraphQLSchema;
import com.datasqrl.util.TestScript;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.stream.Collectors;

public class IndexSelectionUseCaseTest extends AbstractSchemaInferenceModelTest {

  @ParameterizedTest
  @ArgumentsSource(TestScript.AllScriptsWithGraphQLSchemaProvider.class)
  public void fullScriptTest(TestScript script, TestGraphQLSchema graphQLSchema) {
    SnapshotTest.Snapshot snapshot = SnapshotTest.Snapshot.of(getClass(), script.getName(),
        graphQLSchema.getName());
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory());
    String result = selectIndexes(script, graphQLSchema.getSchemaPath()).entrySet().stream()
        .map(e -> e.getKey().getName() + " - " + e.getValue()).sorted()
        .collect(Collectors.joining(System.lineSeparator()));
    snapshot.addContent(result);
    snapshot.createOrValidate();
  }


}
