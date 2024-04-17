///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl.io;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertFalse;
//import static org.junit.jupiter.api.Assertions.assertTrue;
//
//import com.datasqrl.AbstractEngineIT;
//import com.datasqrl.IntegrationTestSettings;
//import com.datasqrl.canonicalizer.Name;
//import com.datasqrl.discovery.DataDiscovery;
//import com.datasqrl.discovery.DataDiscoveryFactory;
//import com.datasqrl.discovery.TableWriter;
//import com.datasqrl.discovery.system.FileSystemDiscovery;
//import com.datasqrl.error.ErrorCollector;
//import com.datasqrl.io.tables.TableSource;
//import com.datasqrl.util.SnapshotTest;
//import com.datasqrl.util.TestDataset;
//import com.datasqrl.util.data.Books;
//import com.datasqrl.util.data.Nutshop;
//import com.datasqrl.util.data.Retail;
//import com.google.common.collect.Iterables;
//import java.util.List;
//import java.util.Optional;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//import lombok.SneakyThrows;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.MethodSource;
//
//@Disabled
//public class TestDataSetMonitoringIT extends AbstractEngineIT {
//  SqrlConfig configuration;
//
//
//  static Stream<TestDataset> argumentProvider() {
//    return Stream.of(Retail.INSTANCE, Nutshop.INSTANCE, Nutshop.COMPRESS);
//  }
//
//  @SneakyThrows
//  @ParameterizedTest
//  @MethodSource("argumentProvider")
//  public void testDatasetMonitoring(TestDataset example) {
//    initialize(
//        IntegrationTestSettings.builder().stream(IntegrationTestSettings.StreamEngine.FLINK)
//            .database(IntegrationTestSettings.DatabaseEngine.POSTGRES)
//            .build(), null, Optional.empty());
//    configuration = injector.getInstance(SqrlConfig.class);
//
//    SnapshotTest.Snapshot snapshot = SnapshotTest.Snapshot.of(getClass(), example.getName());
//
//    List<TableSource> tables = discoverSchema(example);
//    Assertions.assertEquals(example.getNumTables(), tables.size());
//    Assertions.assertEquals(example.getTables(),
//        tables.stream().map(TableSource::getName).map(Name::getCanonical)
//            .collect(Collectors.toSet()));
//
//    //Write out table configurations
//    for (TableSource table : tables) {
//      assertTrue(Iterables.size(table.getConfiguration().getConfig().getKeys()) > 0);
//      snapshot.addContent(table.getSchema().getDefinition(), table.getName().getDisplay() + " schema");
//    }
//
//    snapshot.createOrValidate();
//  }
//
//  private List<TableSource> discoverSchema(TestDataset example) {
////    ErrorCollector errors = ErrorCollector.root();
////    DataDiscovery discovery = DataDiscoveryFactory.fromConfig(configuration, errors);
////    List<TableSource> sourceTables = discovery.runFullDiscovery(new FileSystemDiscovery(), example.getDataDirectory().toString());
////    assertFalse(errors.isFatal(), errors.toString());
////    Assertions.assertEquals(example.getNumTables(), sourceTables.size());
////    return sourceTables;
//    throw new RuntimeException("");
//  }
//
//  /**
//   * This method is only used to generate schemas for testing purposes and is not a test itself
//   */
//  @Test
//  @Disabled
//  public void generateSchema() {
////    generateTableConfigAndSchemaInDataDir(RetailNested.INSTANCE,
////        IntegrationTestSettings.getInMemory());
//    generateTableConfigAndSchemaInDataDir(Books.INSTANCE);
////    generateTableConfigAndSchemaInDataDir(TestDataset.ofSingleFile(Path.of("../../sqrl-repository/repodata/package_mX9HHbUFTgI8XQiJ8PDKMXD_Kno.json")),
////        IntegrationTestSettings.getFlinkWithDB());
//  }
//
//
//  @SneakyThrows
//  public void generateTableConfigAndSchemaInDataDir(TestDataset example) {
//    assertTrue(example.getNumTables() > 0);
//    initialize(IntegrationTestSettings.getFlinkWithDB(), null, Optional.empty());
//    List<TableSource> tables = discoverSchema(example);
//    TableWriter writer = new TableWriter();
//    writer.writeToFile(example.getDataPackageDirectory(), tables);
//  }
//
//  @Test
//  @Disabled("For testing with local data")
//  public void generateSchemaFromDir() {
////    initialize(IntegrationTestSettings.getInMemory(), null, Optional.empty());
////    ErrorCollector errors = ErrorCollector.root();
////    DataDiscovery discovery = DataDiscoveryFactory.fromConfig(configuration, errors);
////    List<TableSource> sourceTables = discovery.runFullDiscovery(new FileSystemDiscovery(), "dataDir");
////    assertFalse(errors.isFatal(), errors.toString());
////    for (TableSource source : sourceTables) {
////      System.out.println(source.getName());
////      System.out.println(source.getSchema().getDefinition());
////    }
//    throw new RuntimeException("");
//  }
//
//}
