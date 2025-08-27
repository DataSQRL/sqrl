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
package com.datasqrl.discovery.preprocessor;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorConf.Context;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.discovery.TableWriter;
import com.datasqrl.discovery.file.FileCompression;
import com.datasqrl.discovery.file.FilenameAnalyzer;
import com.datasqrl.discovery.file.RecordReader;
import com.datasqrl.discovery.stats.DefaultSchemaGenerator;
import com.datasqrl.discovery.stats.SourceTableStatistics;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaHolder;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.SchemaAdjustmentSettings;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

/*
 * Infers the schema for json and csv files and creates a flexible schema for them
 * as well as a table configuration.
 */
@Slf4j
public class FlexibleSchemaInferencePreprocessor implements DiscoveryPreprocessor {

  public static final Set<String> DATA_FILE_EXTENSIONS = Set.of("jsonl", "csv");

  public static final String EVENT_TIME_COLUMN = "event_time";

  private static final FilenameAnalyzer DATA_FILES = FilenameAnalyzer.of(DATA_FILE_EXTENSIONS);

  private final TableWriter writer = new TableWriter();
  private final Optional<ConnectorConf> connectorFactory;

  @Inject
  public FlexibleSchemaInferencePreprocessor(ConnectorFactoryFactory connectorFactoryFactory) {
    this.connectorFactory =
        connectorFactoryFactory.getOptionalConfig(
            SystemBuiltInConnectors.LOCAL_FILE_SOURCE.toString());
  }

  @Override
  public Pattern getPattern() {
    return DATA_FILES.getFilePattern();
  }

  @Override
  public void discoverFile(Path file, ProcessorContext processorContext, ErrorCollector errors) {
    if (connectorFactory.isEmpty()) {
      errors.warn(
          "No connector template defined for local filesystem. Cannot run schema inference.");
      return; // Profile is missing connector for local files
    }
    var match = DATA_FILES.analyze(file);
    if (match.isPresent()) {
      // 1. Setup file reading
      var fileComponents = match.get();
      var parentDir = file.getParent();
      if ((parentDir == null) || !Name.isValidNameStrict(fileComponents.getFilename())) {
        return;
      }
      var tableName = Name.system(fileComponents.getFilename());
      if (hasSchemaOrConfig(parentDir, tableName)) {
        return; // Don't infer schema if it's already present
      }
      var compressor = FileCompression.of(fileComponents.getCompression());
      Optional<RecordReader> reader =
          ServiceLoaderDiscovery.findFirst(
              RecordReader.class, r -> r.getExtensions().contains(fileComponents.getExtension()));
      if (reader.isEmpty()) {
        errors.warn(
            "Could not infer schema for data file [%s] because file reader for extension could not be found: %s",
            file, fileComponents.getExtension());
        return; // Unsupported file type
      }
      if (compressor.isEmpty()) {
        errors.warn(
            "Could not infer schema for data file [%s] because compression codec is not supported: %s",
            file, fileComponents.getCompression());
        return; // Compression not supported
      }
      // 2. Compute table statistics from file records
      Optional<SourceTableStatistics> statistics;
      try (var io = compressor.get().decompress(new FileInputStream(file.toFile()))) {
        var dataflow = reader.get().read(io);
        statistics =
            dataflow
                .flatMap(
                    data -> {
                      var acc = new SourceTableStatistics();
                      var stepErrors = ErrorCollector.root();
                      acc.validate(data, stepErrors);
                      if (!errors.isFatal()) {
                        acc.add(data, null);
                        return Stream.of(acc);
                      } else {
                        log.warn(
                            "Encountered error reading data record: {}\nError: {}",
                            data,
                            stepErrors);
                        return Stream.empty();
                      }
                    })
                .reduce(
                    (base, add) -> {
                      base.merge(add);
                      return base;
                    });
      } catch (IOException e) {
        errors.warn("Could not infer schema of file [%s] due to IO error: %s", file, e);
        return;
      }
      if (statistics.isEmpty() || statistics.get().getCount() == 0) {
        errors.warn(
            "Could not infer schema for data file [%s] because it contained no valid records",
            file);
        return; // No data
      }
      // 3. Infer schema from table statistics
      var generator = new DefaultSchemaGenerator(SchemaAdjustmentSettings.DEFAULT);
      FlexibleTableSchema schema;
      var subErrors = errors.resolve(tableName.getDisplay());
      schema = generator.mergeSchema(statistics.get(), tableName, subErrors);
      if (subErrors.isFatal()) {
        errors.warn("Could not infer schema for file [%s]: %s", file, subErrors);
        return;
      }
      var schemaHolder = new FlexibleTableSchemaHolder(schema);
      // 4. Infer primary key
      var rowType =
          SchemaToRelDataTypeFactory.load(schemaHolder).map(schemaHolder, tableName, errors);
      // We use a conservative method where each simple column is a primary key column
      var primaryKey =
          rowType.getFieldList().stream()
              .filter(
                  f ->
                      !f.getType().isNullable()
                          && CalciteUtil.isPotentialPrimaryKeyType(f.getType()))
              .map(RelDataTypeField::getName)
              .toArray(String[]::new);

      // 5. Create table
      var tblBuilder = new FlinkTableBuilder();
      tblBuilder.setName(tableName);
      tblBuilder.setRelDataType(rowType);
      // Add event time field if not already present
      var eventTimeField = EVENT_TIME_COLUMN;
      RelDataTypeField eventField = rowType.getField(eventTimeField, false, false);
      if (eventField != null) {
        eventTimeField = eventField.getName();
      } else {
        var nowFunction =
            FlinkSqlOperatorTable.dynamicFunctions(false).stream()
                .filter(fct -> fct.isName("now", false))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Could not find now function"));
        tblBuilder.addComputedColumn(eventTimeField, nowFunction);
      }
      // Add watermark
      tblBuilder.setWatermarkMillis(eventTimeField, 1);
      tblBuilder.setConnectorOptions(
          connectorFactory
              .get()
              .toMapWithSubstitution(
                  Context.builder()
                      .tableName(tableName.getDisplay())
                      .tableId(tableName.getDisplay())
                      .filename(file.getFileName().toString())
                      .format(reader.get().getFormat())
                      .build()));

      // 6. Write files and add to observed files
      try {
        var writtenFiles = writer.writeToFile(parentDir, tblBuilder);
        writtenFiles.forEach(processorContext::addDependency);
      } catch (IOException e) {
        errors.fatal("Could not write schema and configuration files: %s", e);
      }
    }
  }

  /**
   * This is a conservative way of checking if a given table already has a schema or configuration
   * in the given directory.
   *
   * <p>We check if there is any file that starts with the tablename followed by an extension
   * separator (i.e. '.').
   *
   * @param dir
   * @param tableName
   * @return
   */
  @SneakyThrows
  private boolean hasSchemaOrConfig(Path dir, Name tableName) {
    int length = tableName.length();
    try (Stream<Path> paths = Files.list(dir)) {
      return paths
          .map(p -> p.getFileName().toString())
          .anyMatch(
              f ->
                  f.length() > length
                      && Name.lower(f.substring(0, length)).equals(tableName)
                      && filterExtensions.contains(f.substring(length).toLowerCase()));
    }
  }

  public static final Set<String> filterExtensions = Set.of(".table.sql", ".schema.yml", ".avsc");
}
