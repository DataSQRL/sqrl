package ai.datasqrl.compile.loaders;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.impl.print.PrintDataSystem;
import ai.datasqrl.io.sources.DataSystem;
import ai.datasqrl.io.sources.DataSystemConfig;
import ai.datasqrl.io.sources.dataset.AbstractExternalTable;
import ai.datasqrl.io.sources.dataset.TableConfig;
import ai.datasqrl.io.sources.dataset.TableSink;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import ai.datasqrl.schema.input.external.SchemaImport;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class DataSourceLoader extends AbstractLoader implements Loader, Exporter {

  public static final String TABLE_FILE_SUFFIX = ".table.json";
  public static final String DATASYSTEM_FILE = "datasystem.json";
  public static final String SCHEMA_FILE_SUFFIX = ".schema.yml";
  public static final String PACKAGE_SCHEMA_FILE = "schema.yml";
  private static final Pattern CONFIG_FILE_PATTERN = Pattern.compile("(.*)\\.table\\.json$");

  @Override
  public Optional<String> handles(Path file) {
    Matcher matcher = CONFIG_FILE_PATTERN.matcher(file.getFileName().toString());
    if (matcher.find()) {
      return Optional.of(matcher.group(1));
    }
    return Optional.empty();
  }

  @Override
  public boolean load(Env env, NamePath fullPath, Optional<Name> alias) {
    return readTable(env.getPackagePath(), fullPath, env.getSession().getErrors()).map(tbl -> env.registerTable(tbl,alias))
            .map(name -> name!=null).orElse(false);
  }

  public Optional<TableSource> readTable(Path rootDir, NamePath fullPath, ErrorCollector errors) {
    return readTable(rootDir, fullPath, errors, TableSource.class);
  }

  @Override
  public Optional<TableSink> export(Env env, NamePath fullPath) {
    Optional<TableSink> sink = readTable(env.getPackagePath(), fullPath, env.getSession().getErrors(), TableSink.class);
    log.info("Sink:" + sink.isEmpty());
    if (sink.isEmpty()) {
      //See if we can discover the sink on-demand
      NamePath basePath = fullPath.subList(0,fullPath.size()-1);
      Name tableName = fullPath.getLast();
      Path baseDir = namepath2Path(env.getPackagePath(), basePath);
      Path datasystempath = baseDir.resolve(DATASYSTEM_FILE);
      DataSystemConfig discoveryConfig;
      log.info("Is regular file: "+ Files.isRegularFile(datasystempath)+" " +
          basePath.getLast().getCanonical() + " " + PrintDataSystem.SYSTEM_TYPE);

      if (Files.isRegularFile(datasystempath)) {
        discoveryConfig = mapJsonFile(datasystempath, DataSystemConfig.class);
      } else if (basePath.size()==1 && basePath.getLast().getCanonical().equals(PrintDataSystem.SYSTEM_TYPE)) {
        discoveryConfig = PrintDataSystem.DEFAULT_DISCOVERY_CONFIG;
      } else {
        return Optional.empty();
      }
      ErrorCollector errors = env.getSession().getErrors();
      DataSystem dataSystem = discoveryConfig.initialize(errors);
      if (dataSystem==null) return Optional.empty();
      return dataSystem.discoverSink(tableName, errors).map(tblConfig ->
              tblConfig.initializeSink(errors, basePath, Optional.empty()));
    } else {
      return sink;
    }
  }

  public<T extends AbstractExternalTable> Optional<T> readTable(Path rootDir, NamePath fullPath, ErrorCollector errors, Class<T> clazz) {
    NamePath basePath = fullPath.subList(0,fullPath.size()-1);
    String tableFileName = fullPath.getLast().getCanonical();
    Path baseDir = namepath2Path(rootDir, basePath);
    Path tableConfigPath = baseDir.resolve(tableFileName + TABLE_FILE_SUFFIX);
    //First, look for table specific schema file. If not present, look for package global schema file
    Path tableSchemaPath = baseDir.resolve(tableFileName + SCHEMA_FILE_SUFFIX);
    if (!Files.isRegularFile(tableSchemaPath)) {
      tableSchemaPath = baseDir.resolve(PACKAGE_SCHEMA_FILE);
    }
    if (!Files.isRegularFile(tableConfigPath)) return Optional.empty();

    //todo: namespace monoid to handle aliasing and exposing to global namespace
    TableConfig tableConfig = mapJsonFile(tableConfigPath, TableConfig.class);
    if (tableConfig.getType()==null) return Optional.empty(); //Invalid configuration

    //Get table schema
    Optional<FlexibleDatasetSchema.TableField> tableSchema = Optional.empty();
    if (Files.isRegularFile(tableSchemaPath)) {
      SchemaDefinition schemaDef = mapYAMLFile(tableSchemaPath, SchemaDefinition.class);
      SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP, tableConfig.getNameCanonicalizer());
      Map<Name, FlexibleDatasetSchema> schemas = importer.convertImportSchema(schemaDef, errors);
      Preconditions.checkArgument(schemaDef.datasets.size() == 1);
      FlexibleDatasetSchema dsSchema = Iterables.getOnlyElement(schemas.values());
      FlexibleDatasetSchema.TableField tbField = dsSchema.getFieldByName(tableConfig.getResolvedName());
      tableSchema = Optional.of(tbField);
    }

    T resultTable;
    if (clazz.equals(TableSource.class)) {
      if (!tableConfig.getType().isSource()) return Optional.empty();
      //TableSource requires a schema
      if (tableSchema.isEmpty()) {
        log.warn("Found configuration for table [%s] but no schema. Table not loaded.", fullPath);
        return Optional.empty();
      }
      resultTable = (T)tableConfig.initializeSource(errors, basePath, tableSchema.get());
    } else if (clazz.equals(TableSink.class)) {
      if (!tableConfig.getType().isSink()) return Optional.empty();
      resultTable = (T)tableConfig.initializeSink(errors, basePath, tableSchema);
    } else throw new UnsupportedOperationException("Invalid table clazz: " + clazz);
    return Optional.of(resultTable);
  }

  public SchemaDefinition loadPackageSchema(Path baseDir) {
    Path tableSchemaPath = baseDir.resolve(PACKAGE_SCHEMA_FILE);
    return mapYAMLFile(tableSchemaPath, SchemaDefinition.class);
  }
}
