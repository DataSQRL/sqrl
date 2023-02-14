/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.AbstractEngineIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.discovery.DataDiscovery;
import com.datasqrl.discovery.TableWriter;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.external.SchemaDefinition;
import com.datasqrl.schema.input.external.SchemaExport;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Quickstart;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.junit.ArgumentProvider;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class TestDataSetMonitoringIT extends AbstractEngineIT {

  @SneakyThrows
  @ParameterizedTest
  @ArgumentsSource(TestDatasetPlusStreamEngine.class)
  public void testDatasetMonitoring(TestDataset example,
      IntegrationTestSettings.EnginePair engine) {
    initialize(
        IntegrationTestSettings.builder().stream(engine.getStream()).database(engine.getDatabase())
            .build());
    SnapshotTest.Snapshot snapshot = SnapshotTest.Snapshot.of(getClass(), example.getName(),
        engine.getName());

    List<TableSource> tables = discoverSchema(example);
    assertEquals(example.getNumTables(), tables.size());
    assertEquals(example.getTables(),
        tables.stream().map(TableSource::getName).map(Name::getCanonical)
            .collect(Collectors.toSet()));

    //Write out table configurations
    for (TableSource table : tables) {
      String json = FileTestUtil.writeJson(table.getConfiguration());
      assertTrue(json.length() > 0);
    }

    Name datasetName = tables.get(0).getPath().parent().getLast();
    FlexibleDatasetSchema combinedSchema = DataDiscovery.combineSchema(tables);

    //Write out combined schema file
    SchemaExport export = new SchemaExport();
    SchemaDefinition outputSchema = export.export(Map.of(datasetName, combinedSchema));
    snapshot.addContent(FileTestUtil.writeYaml(outputSchema), "combined schema");
    snapshot.createOrValidate();
  }

  private List<TableSource> discoverSchema(TestDataset example) {
    ErrorCollector errors = ErrorCollector.root();
    DataDiscovery discovery = new DataDiscovery(errors, engineSettings);
    DataSystemConfig systemConfig = getSystemConfigBuilder(example).build();
    List<TableSource> sourceTables = discovery.runFullDiscovery(systemConfig);
    assertFalse(errors.isFatal(), errors.toString());
    assertEquals(example.getNumTables(), sourceTables.size());
    return sourceTables;
  }

  static class TestDatasetPlusStreamEngine implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {

      IntegrationTestSettings.EnginePair inmem = new IntegrationTestSettings.EnginePair(
          IntegrationTestSettings.DatabaseEngine.INMEMORY,
          IntegrationTestSettings.StreamEngine.INMEMORY);

      IntegrationTestSettings.EnginePair flink = new IntegrationTestSettings.EnginePair(
              IntegrationTestSettings.DatabaseEngine.POSTGRES,
              IntegrationTestSettings.StreamEngine.FLINK);

      List<IntegrationTestSettings.EnginePair> engines = List.of(inmem, flink);
      return Stream.concat(
          ArgumentProvider.crossProduct(List.of(Retail.INSTANCE, Nutshop.INSTANCE), engines),
          Stream.of(Arguments.of(Nutshop.COMPRESS, flink)));
    }
  }

  /**
   * This method is only used to generate schemas for testing purposes and is not a test itself
   */
  @Test
  @Disabled
  public void generateSchema() {
//    generateTableConfigAndSchemaInDataDir(Retail.INSTANCE,
//        IntegrationTestSettings.getInMemory());
    generateTableConfigAndSchemaInDataDir(Quickstart.INSTANCE,
        IntegrationTestSettings.getFlinkWithDB());
  }

  @SneakyThrows
  public void generateTableConfigAndSchemaInDataDir(TestDataset example,
      IntegrationTestSettings settings) {
    assertTrue(example.getNumTables() > 0);
    initialize(settings);
    List<TableSource> tables = discoverSchema(example);
    TableWriter writer = new TableWriter();
    writer.writeToFile(example.getRootPackageDirectory().resolve(example.getName()), tables);
  }

}
