package ai.datasqrl.compile.loaders;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.error.ErrorCode;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.io.sources.dataset.TableConfig;
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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DataSourceLoader extends AbstractLoader implements Loader {

  public static final String CONFIG_FILE_SUFFIX = ".source.json";
  public static final String SCHEMA_FILE_SUFFIX = ".schema.yml";
  private static final Pattern CONFIG_FILE_PATTERN = Pattern.compile("(.*)\\.source\\.json$");

  @Override
  public boolean load(Env env, NamePath fullPath, Optional<Name> alias) {
    NamePath basePath = fullPath.subList(0,fullPath.size()-1);
    Path tableConfigPath = namepath2Path(env, basePath).resolve(fullPath.getLast().getCanonical() + CONFIG_FILE_SUFFIX);
    if (Files.isRegularFile(tableConfigPath)) return loadTable(env, tableConfigPath, basePath, alias)!=null;
    else return false;
  }

  @Override
  public Set<Name> loadAll(Env env, NamePath basePath) {
    return getAllFilesInPath(namepath2Path(env,basePath),CONFIG_FILE_PATTERN).stream().map(p ->
            loadTable(env,p, basePath, Optional.empty())).collect(Collectors.toSet());
  }

  public Name loadTable(Env env, Path tableConfigPath, NamePath basePath, Optional<Name> alias) {
    ErrorCollector errors = env.getSession().getErrors();
    Matcher fileNameMatcher = CONFIG_FILE_PATTERN.matcher(tableConfigPath.getFileName().toString());
    Preconditions.checkState(fileNameMatcher.find(),"Not a valid table filename: %s",tableConfigPath);
    //todo: namespace monoid to handle aliasing and exposing to global namespace
    TableConfig tableConfig = mapJsonFile(tableConfigPath, TableConfig.class);

    //Get table schema
    String tableFileName = fileNameMatcher.group(1);
    SchemaDefinition schemaDef = mapYAMLFile(tableConfigPath.getParent().resolve(tableFileName + SCHEMA_FILE_SUFFIX), SchemaDefinition.class);
    SchemaImport importer = new SchemaImport(Constraint.FACTORY_LOOKUP, tableConfig.getNameCanonicalizer());
    Map<Name,FlexibleDatasetSchema> schemas = importer.convertImportSchema(schemaDef, errors);
    Preconditions.checkArgument(schemaDef.datasets.size()==1);
    FlexibleDatasetSchema dsSchema = Iterables.getOnlyElement(schemas.values());
    FlexibleDatasetSchema.TableField tbField = dsSchema.getFieldByName(tableConfig.getName());


    TableSource tableSource = tableConfig.initializeSource(errors,basePath,tbField);

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
