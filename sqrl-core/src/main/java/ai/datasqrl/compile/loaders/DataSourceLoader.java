package ai.datasqrl.compile.loaders;

import ai.datasqrl.config.error.ErrorCode;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.dataset.TableConfig;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.parse.Check;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import ai.datasqrl.schema.input.external.SchemaImport;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataSourceLoader extends AbstractLoader implements Loader {

  public static final String CONFIG_FILE_SUFFIX = ".source.json";
  public static final String SCHEMA_FILE_SUFFIX = ".schema.yml";
  public static final String PACKAGE_SCHEMA_FILE = "schema.yml";
  private static final Pattern CONFIG_FILE_PATTERN = Pattern.compile("(.*)\\.source\\.json$");

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
    return readTable(env.getPackagePath(), fullPath, env.getSession().getErrors()).map(tbl -> registerTable(env,tbl,alias))
            .map(name -> name!=null).orElse(false);
  }


  public Optional<TableSource> readTable(Path rootDir, NamePath fullPath, ErrorCollector errors) {
    NamePath basePath = fullPath.subList(0,fullPath.size()-1);
    String tableFileName = fullPath.getLast().getCanonical();
    Path baseDir = namepath2Path(rootDir, basePath);
    Path tableConfigPath = baseDir.resolve(tableFileName + CONFIG_FILE_SUFFIX);
    //First, look for table specific schema file. If not present, look for package global schema file
    Path tableSchemaPath = baseDir.resolve(tableFileName + SCHEMA_FILE_SUFFIX);
    if (!Files.isRegularFile(tableSchemaPath)) {
      tableSchemaPath = baseDir.resolve(PACKAGE_SCHEMA_FILE);
    }
    if (!Files.isRegularFile(tableConfigPath) || !Files.isRegularFile(tableSchemaPath)) return Optional.empty();

    //todo: namespace monoid to handle aliasing and exposing to global namespace
    TableConfig tableConfig = mapJsonFile(tableConfigPath, TableConfig.class);

    //Get table schema
    SchemaDefinition schemaDef = mapYAMLFile(tableSchemaPath, SchemaDefinition.class);
    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP, tableConfig.getNameCanonicalizer());
    Map<Name,FlexibleDatasetSchema> schemas = importer.convertImportSchema(schemaDef, errors);
    Preconditions.checkArgument(schemaDef.datasets.size()==1);
    FlexibleDatasetSchema dsSchema = Iterables.getOnlyElement(schemas.values());
    FlexibleDatasetSchema.TableField tbField = dsSchema.getFieldByName(tableConfig.getName());


    TableSource tableSource = tableConfig.initializeSource(errors,basePath,tbField);
    return Optional.of(tableSource);
  }

  public SchemaDefinition loadPackageSchema(Path baseDir) {
    Path tableSchemaPath = baseDir.resolve(PACKAGE_SCHEMA_FILE);
    return mapYAMLFile(tableSchemaPath, SchemaDefinition.class);
  }

  private Name registerTable(Env env, TableSource tableSource, Optional<Name> alias) {
    ScriptTableDefinition def = createScriptTableDefinition(env, tableSource, alias);

    if (env.getRelSchema()
            .getTable(def.getTable().getName().getCanonical(), false) != null) {
      throw Check.newException(ErrorCode.IMPORT_NAMESPACE_CONFLICT,
              env.getCurrentNode(),
              env.getCurrentNode().getParserPosition(),
              String.format("An item named `%s` is already in scope",
                      def.getTable().getName().getDisplay()));
    }

    Resolve.registerScriptTable(env, def);
    return tableSource.getName();
  }

  private ScriptTableDefinition createScriptTableDefinition(Env env, TableSource tableSource,
      Optional<Name> alias) {
    return env.getTableFactory().importTable(tableSource, alias,
        env.getSession().getPlanner().getRelBuilder(), env.getSession().getPipeline());
  }

}
