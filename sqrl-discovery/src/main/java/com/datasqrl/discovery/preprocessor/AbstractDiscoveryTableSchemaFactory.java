package com.datasqrl.discovery.preprocessor;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.util.FileCompression;
import com.datasqrl.util.FilenameAnalyzer;
import com.datasqrl.discovery.file.RecordReader;
import com.datasqrl.discovery.stats.DefaultSchemaGenerator;
import com.datasqrl.discovery.stats.SourceTableStatistics;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.SchemaConversionResult;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaFactory;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.SchemaAdjustmentSettings;
import com.datasqrl.io.schema.TableSchemaFactory;
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
    var compressor = FileCompression.of(fileComponents.getCompression());
    Optional<RecordReader> reader =
        ServiceLoaderDiscovery.findFirst(
            RecordReader.class, r -> r.getExtensions().contains(fileComponents.getExtension()));
    localErrors.checkFatal(
        reader.isPresent(),
        "Could not load reader for file extension: %s",
        fileComponents.getExtension());
    localErrors.checkFatal(
        compressor.isPresent(),
        "Could not infer schema for data file [%s] because compression codec is not supported: %s",
        location,
        fileComponents.getCompression());
    // 2. Compute table statistics from file records
    Optional<SourceTableStatistics> statistics;
    try (var io = compressor.get().decompress(new FileInputStream(location.toFile()))) {
      var dataflow = reader.get().read(io);
      statistics =
          dataflow
              .flatMap(
                  data -> {
                    var acc = new SourceTableStatistics();
                    var stepErrors = ErrorCollector.root();
                    acc.validate(data, stepErrors);
                    if (!stepErrors.isFatal()) {
                      acc.add(data, null);
                      return Stream.of(acc);
                    } else {
                      log.warn(
                          "Encountered error reading data record: {}\nError: {}", data, stepErrors);
                      return Stream.empty();
                    }
                  })
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
    String tableName = location.getFileName().toString();
    schema = generator.mergeSchema(statistics.get(), Name.system(tableName), localErrors);

    var rowType = FlexibleTableSchemaFactory.convert(schema, tableName);
    Map<String, String> options = new LinkedHashMap<>();
    options.put("connector", "filesystem");
    options.put("format", reader.get().getFormat());
    options.put("path", "${" + DATA_PATH + "}/" + location.getFileName().toString());
    return new SchemaConversionResult(rowType, options);
  }
}
