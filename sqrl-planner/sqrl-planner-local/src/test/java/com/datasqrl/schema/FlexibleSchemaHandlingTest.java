/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.engine.stream.flink.schema.UniversalTable2FlinkSchema;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.FlexibleTable2UTBConverter;
import com.datasqrl.schema.input.FlexibleTableConverter;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.schema.input.external.SchemaImport;
import com.datasqrl.model.schema.TableDefinition;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestDataset;
import com.datasqrl.util.junit.ArgumentProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests the generation of schemas for various consumers based on the central
 * {@link FlexibleTableSchema} by way of the {@link UniversalTable}.
 */
public class FlexibleSchemaHandlingTest {

  @ParameterizedTest
  @ArgumentsSource(SchemaConverterProvider.class)
  public <S> void conversionTest(InputSchema inputSchema, SchemaConverterTestCase<S> visitorTest) {
    SnapshotTest.Snapshot snapshot = SnapshotTest.Snapshot.of(getClass(), inputSchema.getName(),
        visitorTest.schemaConverter.getClass().getSimpleName());
    Name tableAlias = Name.system("TestTable");
    List<FlexibleTableSchema> schemas = getSchemas(inputSchema);
    for (FlexibleTableSchema table : schemas) {
      for (boolean hasSourceTimestamp : new boolean[]{true, false}) {
        for (Optional<Name> alias : new Optional[]{Optional.empty(), Optional.of(tableAlias)}) {
          FlexibleTableConverter converter = new FlexibleTableConverter(
              table, hasSourceTimestamp, alias);
          FlexibleTable2UTBConverter utbConverter = new FlexibleTable2UTBConverter(false);
          UniversalTable tblBuilder = converter.apply(utbConverter);
          if (alias.isPresent()) {
            assertEquals(tblBuilder.getName(), alias.get());
            continue;
          }
          S resultSchema = visitorTest.schemaConverter.convertSchema(tblBuilder);
          assertNotNull(resultSchema);
          String[] caseName = getCaseName(table.getName().getDisplay(), hasSourceTimestamp);
          snapshot.addContent(resultSchema.toString(), caseName);
        }
      }
    }
    snapshot.createOrValidate();
  }

  public static String[] getCaseName(String tableName, boolean hasTimestamp) {
    List<String> caseName = new ArrayList<>();
    caseName.add(tableName);
    if (hasTimestamp) {
      caseName.add("hasTimestamp");
    }
    return caseName.toArray(new String[caseName.size()]);
  }

  @SneakyThrows
  public List<FlexibleTableSchema> getSchemas(InputSchema inputSchema) {
    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP, NameCanonicalizer.SYSTEM);
    ErrorCollector errors = ErrorCollector.root();
    Deserializer deserializer = new Deserializer();
    List<FlexibleTableSchema> schemas = Files.list(inputSchema.packageDir)
            .sorted()
            .filter(f -> f.getFileName().toString().endsWith(FlexibleTableSchemaFactory.SCHEMA_EXTENSION))
            .map(f -> deserializer.mapYAMLFile(f, TableDefinition.class))
            .map(td -> importer.convert(td, errors).get())
            .collect(Collectors.toList());

    assertFalse(errors.isFatal(), errors.toString());
    assertFalse(schemas.isEmpty());
    return schemas;
  }


  @Value
  public static class InputSchema {

    Path packageDir;
    String name;
  }

  static class SchemaConverterProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      List<SchemaConverterTestCase> converters = new ArrayList<>();

      CalciteTableFactory calciteTableFactory = new CalciteTableFactory();
//      SqrlQueryPlanner planner = new SqrlQueryPlanner(new SqrlSchema(), new FlinkBackedFunctionCatalog());
      //Calcite
      CalciteTableFactory.UTB2RelDataTypeConverter converter = calciteTableFactory.new UTB2RelDataTypeConverter();
      converters.add(new SchemaConverterTestCase(converter));
      //Flink
      converters.add(new SchemaConverterTestCase(new FlinkTypeInfoSchemaGenerator()));
      converters.add(new SchemaConverterTestCase(new UniversalTable2FlinkSchema()));

      List<InputSchema> schemas = TestDataset.getAll().stream()
          .map(td -> new InputSchema(td.getDataPackageDirectory(),
              td.getName()))
          .collect(Collectors.toList());

      return ArgumentProvider.crossProduct(schemas, converters);
    }
  }

  @Value
  @AllArgsConstructor
  static class SchemaConverterTestCase<S> {

    UniversalTable.SchemaConverter<S> schemaConverter;
    SchemaConverterValidator<S> validator;

    public SchemaConverterTestCase(UniversalTable.SchemaConverter<S> schemaConverter) {
      this(schemaConverter, null);
    }

  }

  @FunctionalInterface
  static interface SchemaConverterValidator<S> {

    void validate(S result, Name tableName);

  }


}
