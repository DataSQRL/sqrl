/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.table.TableConverter;
import com.datasqrl.plan.table.UTB2RelDataTypeConverter;
import com.datasqrl.schema.UniversalTable.SchemaConverterUTB;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.converters.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.schema.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.schema.converters.UniversalTable2FlinkSchema;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.datasqrl.schema.input.external.SchemaImport;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.serializer.Deserializer;
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
import org.apache.calcite.rel.type.RelDataType;
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
        stripLambdaName(visitorTest.schemaConverter.getClass().getSimpleName()));
    Name tableAlias = Name.system("TestTable");
    List<FlexibleTableSchema> schemas = getSchemas(inputSchema);
    for (FlexibleTableSchema table : schemas) {
      for (boolean hasSourceTimestamp : new boolean[]{true, false}) {
        for (Optional<Name> alias : new Optional[]{Optional.empty(), Optional.of(tableAlias)}) {
          Name tableName = alias.orElse(table.getName());
          ErrorCollector errors = ErrorCollector.root();
          FlexibleTableSchemaHolder tableSchema = new FlexibleTableSchemaHolder(table);
          RelDataType dataType = SchemaToRelDataTypeFactory.load(tableSchema)
              .map(tableSchema, tableName, errors);
          assertFalse(errors.hasErrors(), errors.toString());
          if (alias.isPresent()) {
            continue;
          }
          S resultSchema = visitorTest.schemaConverter.convertSchema(dataType);
          assertNotNull(resultSchema);
          String[] caseName = getCaseName(table.getName().getDisplay(), hasSourceTimestamp);
          snapshot.addContent(resultSchema.toString(), caseName);
        }
      }
    }
    snapshot.createOrValidate();
  }

  private String stripLambdaName(String simpleName) {
    int i = simpleName.indexOf('$');
    return i == -1 ? simpleName : simpleName.substring(0, i);
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
    SqrlFramework framework = new SqrlFramework();

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      List<SchemaConverterTestCase> converters = new ArrayList<>();

      //Calcite
      SchemaConverter<RelDataType> converter = d -> d;
      converters.add(new SchemaConverterTestCase(converter));
      //Flink
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

    SchemaConverter<S> schemaConverter;
    SchemaConverterValidator<S> validator;

    public SchemaConverterTestCase(SchemaConverter<S> schemaConverter) {
      this(schemaConverter, null);
    }

  }

  @FunctionalInterface
  static interface SchemaConverterValidator<S> {

    void validate(S result, Name tableName);

  }


}
