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
import com.datasqrl.discovery.file.RecordReader;
import com.datasqrl.discovery.stats.DefaultSchemaGenerator;
import com.datasqrl.discovery.stats.SourceTableStatistics;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.SchemaConversionResult;
import com.datasqrl.io.schema.TableSchemaFactory;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaFactory;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.SchemaAdjustmentSettings;
import com.datasqrl.util.FileCompression;
import com.datasqrl.util.FilenameAnalyzer;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractDiscoveryTableSchemaFactory implements TableSchemaFactory {

  // fixme: @Ferenc, this and the UDF path should be in a central static variable so they are
  // defined once and used everywhere
  public static final String DATA_PATH = "DATA_PATH";

  @Override
  public SchemaConversionResult convert(Path location, ErrorCollector errors) {
    var localErrors = errors.withConfig(location);
    var matcher = FilenameAnalyzer.of(getExtensions());
    var match = matcher.analyze(location);
    localErrors.checkFatal(match.isPresent(), "Could not analyze the filename for: %s", location);

    // 1. Setup file reading
    var fileComponents = match.get();
    var compressionOpt = FileCompression.of(fileComponents.compression());
    var readerOpt =
        ServiceLoaderDiscovery.findFirst(
            RecordReader.class, r -> r.getExtensions().contains(fileComponents.extension()));
    localErrors.checkFatal(
        readerOpt.isPresent(),
        "Could not load reader for file extension: %s",
        fileComponents.extension());
    localErrors.checkFatal(
        compressionOpt.isPresent(),
        "Could not infer schema for data file [%s] because compression codec is not supported: %s",
        location,
        fileComponents.compression());

    // 2. Compute table statistics from file records
    Optional<SourceTableStatistics> statistics;
    try (var io = compressionOpt.get().decompress(new FileInputStream(location.toFile()))) {
      var dataflow = readerOpt.get().read(io);
      statistics =
          dataflow
              .flatMap(this::getTableStatistics)
              .reduce(
                  (base, add) -> {
                    base.merge(add);
                    return base;
                  });

    } catch (IOException e) {
      throw localErrors.exception(
          "Could not infer schema of file [%s] due to IO error: %s", location, e);
    }
    localErrors.checkFatal(
        statistics.isPresent() && statistics.get().getCount() > 0,
        "Could not infer schema for data file [%s] because it contained no valid records",
        location);

    // 3. Infer schema from table statistics
    var generator = new DefaultSchemaGenerator(SchemaAdjustmentSettings.DEFAULT);
    FlexibleTableSchema schema;
    var tableName = location.getFileName().toString();
    schema = generator.mergeSchema(statistics.get(), Name.system(tableName), localErrors);

    var rowType = FlexibleTableSchemaFactory.convert(schema, tableName);
    var options = new LinkedHashMap<String, String>();
    options.put("connector", "filesystem");
    options.put("format", readerOpt.get().getFormat());
    options.put("path", "${" + DATA_PATH + "}/" + location.getFileName().toString());

    return new SchemaConversionResult(rowType, options);
  }

  private Stream<SourceTableStatistics> getTableStatistics(Map<String, Object> data) {
    var acc = new SourceTableStatistics();
    var stepErrors = ErrorCollector.root();

    acc.validate(data, stepErrors);

    if (stepErrors.isFatal()) {
      log.warn("Encountered error reading data record: {}\nError: {}", data, stepErrors);

      return Stream.empty();
    }

    acc.add(data, null);
    return Stream.of(acc);
  }
}
