/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.AbstractEngineIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.discovery.DataDiscovery;
import com.datasqrl.discovery.DataDiscoveryFactory;
import com.datasqrl.discovery.TableWriter;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.datasqrl.schema.input.external.SchemaExport;
import com.datasqrl.model.schema.TableDefinition;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Sensors;
import com.datasqrl.util.junit.ArgumentProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class TestDataSetMonitoringIT extends AbstractEngineIT {

  private PipelineFactory pipelineFactory;

  @Override
  protected PipelineFactory initialize(IntegrationTestSettings settings) {
    this.pipelineFactory = super.initialize(settings);
    return this.pipelineFactory;
  }

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

    SchemaExport export = new SchemaExport();
    //Write out table configurations
    for (TableSource table : tables) {
      assertTrue(Iterables.size(table.getConfiguration().getConfig().getKeys()) > 0);
      snapshot.addContent(table.getSchema().getDefinition(), table.getName().getDisplay() + " schema");
    }

    snapshot.createOrValidate();
  }

  private List<TableSource> discoverSchema(TestDataset example) {
    ErrorCollector errors = ErrorCollector.root();
    DataDiscovery discovery = DataDiscoveryFactory.fromPipeline(pipelineFactory, errors);
    List<TableSource> sourceTables = discovery.runFullDiscovery(example.getDiscoveryConfig());
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
//    generateTableConfigAndSchemaInDataDir(RetailNested.INSTANCE,
//        IntegrationTestSettings.getInMemory());
    generateTableConfigAndSchemaInDataDir(Sensors.INSTANCE_EPOCH,
        IntegrationTestSettings.getFlinkWithDB());
//    generateTableConfigAndSchemaInDataDir(TestDataset.ofSingleFile(Path.of("../../sqrl-repository/repodata/package_mX9HHbUFTgI8XQiJ8PDKMXD_Kno.json")),
//        IntegrationTestSettings.getFlinkWithDB());
  }


  @SneakyThrows
  public void generateTableConfigAndSchemaInDataDir(TestDataset example,
      IntegrationTestSettings settings) {
    assertTrue(example.getNumTables() > 0);
    initialize(settings);
    List<TableSource> tables = discoverSchema(example);
    TableWriter writer = new TableWriter();
    writer.writeToFile(example.getDataPackageDirectory(), tables);
  }

}
