/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaFactory;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaHolder;
import com.datasqrl.io.schema.flexible.constraint.Constraint;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.schema.flexible.external.SchemaImport;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.external.TableDefinition;
import com.datasqrl.plan.table.SchemaConverter;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests the generation of schemas for various consumers based on the central {@link
 * FlexibleTableSchema}.
 */
@Disabled
public class FlexibleSchemaHandlingTest {

  @ParameterizedTest
  @ArgumentsSource(SchemaConverterProvider.class)
  <S> void conversionTest(InputSchema inputSchema, SchemaConverterTestCase<S> visitorTest) {
    var snapshot =
        SnapshotTest.Snapshot.of(
            getClass(),
            inputSchema.getName(),
            stripLambdaName(visitorTest.schemaConverter.getClass().getSimpleName()));
    var tableAlias = Name.system("TestTable");
    var schemas = getSchemas(inputSchema);
    for (FlexibleTableSchema table : schemas) {
      for (boolean hasSourceTimestamp : new boolean[] {true, false}) {
        for (Optional<Name> alias : new Optional[] {Optional.empty(), Optional.of(tableAlias)}) {
          var tableName = alias.orElse(table.getName());
          var errors = ErrorCollector.root();
          var tableSchema = new FlexibleTableSchemaHolder(table);
          var dataType =
              SchemaToRelDataTypeFactory.load(tableSchema).map(tableSchema, tableName, errors);
          assertThat(errors.hasErrors()).as(errors.toString()).isFalse();
          if (alias.isPresent()) {
            continue;
          }
          var resultSchema = visitorTest.schemaConverter.convertSchema(dataType);
          assertThat(resultSchema).isNotNull();
          var caseName = getCaseName(table.getName().getDisplay(), hasSourceTimestamp);
          snapshot.addContent(resultSchema.toString(), caseName);
        }
      }
    }
    snapshot.createOrValidate();
  }

  private String stripLambdaName(String simpleName) {
    var i = simpleName.indexOf('$');
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
    Deserializer deserializer = Deserializer.INSTANCE;
    List<FlexibleTableSchema> schemas =
        Files.list(inputSchema.packageDir)
            .sorted()
            .filter(
                f ->
                    f.getFileName()
                        .toString()
                        .endsWith(FlexibleTableSchemaFactory.SCHEMA_EXTENSION))
            .map(f -> deserializer.mapYAMLFile(f, TableDefinition.class))
            .map(td -> importer.convert(td, errors).get())
            .collect(Collectors.toList());

    assertThat(errors.isFatal()).as(errors.toString()).isFalse();
    assertThat(schemas).isNotEmpty();
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

      // Calcite
      SchemaConverter<RelDataType> converter = d -> d;
      converters.add(new SchemaConverterTestCase(converter));
      // Flink
      //      converters.add(new SchemaConverterTestCase(new UniversalTable2FlinkSchema()));

      List<InputSchema> schemas =
          TestDataset.getAll().stream()
              .map(td -> new InputSchema(td.getDataPackageDirectory(), td.getName()))
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
