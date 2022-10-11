package ai.datasqrl.compile.loaders;

import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.external.DatasetDefinition;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import ai.datasqrl.schema.input.external.SchemaImport;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Optional;
import java.util.regex.Pattern;

public class DataSourceLoader implements Loader {

  SchemaLoader discoveredSchemaLoader = new DiscoveredSchemaLoader();
  private static final Pattern PATTERN = Pattern.compile(".*\\.source\\.json");

  @Override
  public boolean handles(URI uri, String name) {
    URI file = uri.resolve(name + ".source.json");
    File f = (new File(file));
    return f.exists();
  }

  @Override
  public boolean handlesFile(URI uri, String name) {
    URI file = uri.resolve(name);
    File f = (new File(file));
    return f.exists() && PATTERN.matcher(name).find();
  }

  @Override
  public void load(Env env, URI uri, String name, Optional<Name> alias) {
    loadModule(env, uri, name + ".source.json", alias);
  }

  @Override
  public void loadFile(Env env, URI uri, String name) {
    loadModule(env, uri, name, Optional.empty());
  }

  public void loadModule(Env env, URI uri, String fileName, Optional<Name> alias) {
    //todo: namespace monoid to handle aliasing and exposing to global namespace
    ObjectMapper mapper = new ObjectMapper();
    SourceTable table = resolveUri(uri, fileName, mapper, SourceTable.class);

    SchemaDefinition schemaDef = discoveredSchemaLoader.resolve(uri, fileName.split("\\.")[0]);

    DatasetDefinition definition = schemaDef.datasets.get(0);

    SchemaImport importer = new SchemaImport(null, Constraint.FACTORY_LOOKUP);
    FlexibleDatasetSchema userDSSchema = importer.convert(definition, table.getDataset(), env.getSession().getErrors());

    FlexibleDatasetSchema.TableField tbField = ImportManager.createTable(table,
        userDSSchema.getFieldByName(table.getName()), env.getSchemaAdjustmentSettings(), env.getSession()
            .getErrors().resolve(table.getDataset().getName()));

    SourceTableImport sourceTableImport = new SourceTableImport(table, tbField, env.getSchemaAdjustmentSettings());

    ScriptTableDefinition def = createScriptTableDefinition(env, sourceTableImport, alias);

    Resolve.registerScriptTable(env, def);
  }

  public static <T> T resolveUri(URI uri, String name, ObjectMapper mapper,
      Class<T> clazz) {
    try {
      URL tableURL = uri.resolve(name).toURL();
      return mapper.readValue(tableURL, clazz);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ScriptTableDefinition createScriptTableDefinition(Env env, SourceTableImport tblImport,
      Optional<Name> alias) {
    return env.getTableFactory().importTable(tblImport, alias,
        env.getSession().getPlanner().getRelBuilder());
  }
}
