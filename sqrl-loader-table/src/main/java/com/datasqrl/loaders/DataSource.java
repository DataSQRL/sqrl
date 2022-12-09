/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystem;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.io.tables.AbstractExternalTable;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.external.SchemaDefinition;
import com.datasqrl.schema.input.external.SchemaImport;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataSource {

  public static final String TABLE_FILE_SUFFIX = ".table.json";
  public static final String DATASYSTEM_FILE = "datasystem.json";
  public static final String SCHEMA_FILE_SUFFIX = ".schema.yml";
  public static final String PACKAGE_SCHEMA_FILE = "schema.yml";
  private static final Pattern CONFIG_FILE_PATTERN = Pattern.compile("(.*)\\.table\\.json$");

  protected static <T extends AbstractExternalTable> Optional<T> readTable(Path rootDir,
      NamePath fullPath,
      ErrorCollector errors, Class<T> clazz, Deserializer deserialize) {
    NamePath basePath = fullPath.subList(0, fullPath.size() - 1);
    String tableFileName = fullPath.getLast().getCanonical();
    Path baseDir = AbstractLoader.namepath2Path(rootDir, basePath);
    Path tableConfigPath = baseDir.resolve(tableFileName + TABLE_FILE_SUFFIX);
    //First, look for table specific schema file. If not present, look for package global schema file
    Path tableSchemaPath = baseDir.resolve(tableFileName + SCHEMA_FILE_SUFFIX);
    if (!Files.isRegularFile(tableSchemaPath)) {
      tableSchemaPath = baseDir.resolve(PACKAGE_SCHEMA_FILE);
    }
    if (!Files.isRegularFile(tableConfigPath)) {
      return Optional.empty();
    }

    //todo: namespace monoid to handle aliasing and exposing to global namespace
    TableConfig tableConfig = deserialize.mapJsonFile(tableConfigPath, TableConfig.class);
    if (tableConfig.getType() == null) {
      return Optional.empty(); //Invalid configuration
    }

    //Get table schema
    Optional<FlexibleDatasetSchema.TableField> tableSchema = Optional.empty();
    if (Files.isRegularFile(tableSchemaPath)) {
      SchemaDefinition schemaDef = deserialize.mapYAMLFile(tableSchemaPath, SchemaDefinition.class);
      SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP,
          tableConfig.getNameCanonicalizer());
      Map<Name, FlexibleDatasetSchema> schemas = importer.convertImportSchema(schemaDef, errors);
      Preconditions.checkArgument(schemaDef.datasets.size() == 1);
      FlexibleDatasetSchema dsSchema = Iterables.getOnlyElement(schemas.values());
      FlexibleDatasetSchema.TableField tbField = dsSchema.getFieldByName(
          tableConfig.getResolvedName());
      tableSchema = Optional.of(tbField);
    }

    T resultTable;
    if (clazz.equals(TableSource.class)) {
      if (!tableConfig.getType().isSource()) {
        return Optional.empty();
      }
      //TableSource requires a schema
      if (tableSchema.isEmpty()) {
        log.warn("Found configuration for table [%s] but no schema. Table not loaded.", fullPath);
        return Optional.empty();
      }
      resultTable = (T) tableConfig.initializeSource(errors, basePath, tableSchema.get());
    } else if (clazz.equals(TableSink.class)) {
      if (!tableConfig.getType().isSink()) {
        return Optional.empty();
      }
      resultTable = (T) tableConfig.initializeSink(errors, basePath, tableSchema);
    } else {
      throw new UnsupportedOperationException("Invalid table clazz: " + clazz);
    }
    return Optional.of(resultTable);
  }

  public static class Exporter extends DataSource implements com.datasqrl.loaders.Exporter {

    private final Deserializer deserialize = new Deserializer();

    @Override
    public boolean isPackage(Path packageBasePath, NamePath fullPath) {
      return AbstractLoader.isPackagePath(packageBasePath, fullPath);
    }

    @Override
    public boolean usesFile(Path file) {
      return file.getFileName().toString().endsWith(SCHEMA_FILE_SUFFIX) ||
          file.getFileName().toString().equals(PACKAGE_SCHEMA_FILE) ||
          file.getFileName().toString().endsWith(TABLE_FILE_SUFFIX) ||
          file.getFileName().toString().equals(DATASYSTEM_FILE);
    }

    @Override
    public Optional<TableSink> export(LoaderContext ctx, NamePath fullPath) {
      Optional<TableSink> sink = readTable(ctx.getPackagePath(), fullPath, ctx.getErrorCollector(),
          TableSink.class, deserialize);
      if (sink.isEmpty()) {
        //See if we can discover the sink on-demand
        NamePath basePath = fullPath.subList(0, fullPath.size() - 1);
        Name tableName = fullPath.getLast();
        Path baseDir = AbstractLoader.namepath2Path(ctx.getPackagePath(), basePath);
        Path datasystempath = baseDir.resolve(DATASYSTEM_FILE);
        DataSystemConfig discoveryConfig;

        if (Files.isRegularFile(datasystempath)) {
          discoveryConfig = deserialize.mapJsonFile(datasystempath, DataSystemConfig.class);
        } else if (basePath.size() == 1 && basePath.getLast().getCanonical()
            .equals(PrintDataSystem.SYSTEM_TYPE)) {
          discoveryConfig = PrintDataSystem.DEFAULT_DISCOVERY_CONFIG;
        } else {
          return Optional.empty();
        }
        DataSystem dataSystem = discoveryConfig.initialize(ctx.getErrorCollector());
        if (dataSystem == null) {
          return Optional.empty();
        }
        return dataSystem.discoverSink(tableName, ctx.getErrorCollector()).map(tblConfig ->
            tblConfig.initializeSink(ctx.getErrorCollector(), basePath, Optional.empty()));
      } else {
        return sink;
      }
    }

  }

  public static class Loader extends AbstractLoader {

    @Override
    public Optional<String> loadsFile(Path file) {
      Matcher matcher = CONFIG_FILE_PATTERN.matcher(file.getFileName().toString());
      if (matcher.find()) {
        return Optional.of(matcher.group(1));
      }
      return Optional.empty();
    }

    @Override
    public boolean usesFile(Path file) {
      return super.usesFile(file) || file.getFileName().toString().endsWith(SCHEMA_FILE_SUFFIX) ||
          file.getFileName().toString().equals(PACKAGE_SCHEMA_FILE);
    }

    @Override
    public boolean isPackage(Path packageBasePath, NamePath fullPath) {
      return AbstractLoader.isPackagePath(packageBasePath,fullPath);
    }

    @Override
    public boolean load(LoaderContext ctx, NamePath fullPath, Optional<Name> alias) {
      return readTable(ctx.getPackagePath(), fullPath, ctx.getErrorCollector()).map(
              tbl -> ctx.registerTable(tbl, alias))
          .map(name -> name != null).orElse(false);
    }

    public Optional<TableSource> readTable(Path rootDir, NamePath fullPath, ErrorCollector errors) {
      return DataSource.readTable(rootDir, fullPath, errors, TableSource.class, deserialize);
    }


    public SchemaDefinition loadPackageSchema(Path baseDir) {
      Path tableSchemaPath = baseDir.resolve(PACKAGE_SCHEMA_FILE);
      return deserialize.mapYAMLFile(tableSchemaPath, SchemaDefinition.class);
    }


  }

}
