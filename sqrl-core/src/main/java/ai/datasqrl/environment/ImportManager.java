package ai.datasqrl.environment;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SchemaGenerator;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.InputTableSchema;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import ai.datasqrl.schema.input.external.SchemaImport;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;

import java.util.*;
import org.apache.calcite.schema.Schema;

public class ImportManager {

  public static final NameCanonicalizer IMPORT_CANONICALIZER = NameCanonicalizer.SYSTEM;

  private final DatasetRegistry datasetRegistry;
  private Map<Name, FlexibleDatasetSchema> userSchema = Collections.EMPTY_MAP;
  private final Map<Name, Schema> scriptSchemas = new HashMap<>();

  public ImportManager(DatasetRegistry datasetRegistry) {
    this.datasetRegistry = datasetRegistry;
  }

  public DatasetRegistry getDatasetRegistry() {
    return datasetRegistry;
  }

  public boolean registerUserSchema(SchemaDefinition yamlSchema, ErrorCollector errors) {
    SchemaImport importer = new SchemaImport(datasetRegistry, Constraint.FACTORY_LOOKUP);
    Map<Name, FlexibleDatasetSchema> schema = importer.convertImportSchema(yamlSchema, errors);
    if (errors.isFatal()) return false;
    if (!schema.isEmpty()) {
      registerUserSchema(schema);
    }
    return true;
  }

  public void registerUserSchema(Map<Name, FlexibleDatasetSchema> schema) {
    Preconditions.checkArgument(schema != null && !schema.isEmpty(), "Schema is empty");
    this.userSchema = schema;
  }

  public void registerScript(@NonNull Name name,
      @NonNull Schema scriptSchema) {
    Preconditions.checkArgument(scriptSchemas.containsKey(name),
        "Duplicate name for script import: %s", name);
    scriptSchemas.put(name, scriptSchema);
  }

  public List<TableImport> importAllTables(@NonNull Name datasetName,
      SchemaAdjustmentSettings schemaAdjustmentSettings,
      ErrorCollector errors) {
    List<TableImport> imports = new ArrayList<>();
    if (scriptSchemas.containsKey(datasetName)) {
      throw new UnsupportedOperationException("Not yet implemented");
    } else {
      SourceDataset dsDef = datasetRegistry.getDataset(datasetName);
      if (dsDef == null) {
        errors.fatal("Unknown dataset: %s", datasetName);
        return imports; //abort
      }
      for (SourceTable table : dsDef.getTables()) {
        TableImport tblimport = importTable(datasetName, table.getName(), schemaAdjustmentSettings, errors);
        if (tblimport != null) {
          imports.add(tblimport);
        }
      }
    }
    return imports;
  }

  public TableImport importTable(@NonNull Name datasetName, @NonNull Name tableName,
      SchemaAdjustmentSettings schemaAdjustmentSettings,
      ErrorCollector errors) {
    if (scriptSchemas.containsKey(datasetName)) {
      throw new UnsupportedOperationException("Not yet implemented");
    } else {
      SourceDataset dsDef = datasetRegistry.getDataset(datasetName);
      if (dsDef == null) {
        errors.fatal("Unknown dataset: %s", datasetName);
        return null; //abort
      }
      errors = errors.resolve(datasetName);
      SourceTable table = dsDef.getTable(tableName);
      if (table == null) {
        errors.fatal("Unknown table: %s", tableName);
        return null;
      }
      FlexibleDatasetSchema userDSSchema = userSchema.get(datasetName);
      if (userDSSchema == null) {
        userDSSchema = FlexibleDatasetSchema.EMPTY;
      }
      FlexibleDatasetSchema.TableField tbField = createTable(table, datasetName,
          userDSSchema.getFieldByName(table.getName()), schemaAdjustmentSettings, errors);
      System.out.println(errors);
      //schemaConverter.convert(tbField,imp.asName.orElse(table.getName()))
      return new SourceTableImport(table, tbField, schemaAdjustmentSettings);
    }
  }

  public interface TableImport {

    boolean isSource();

    default boolean isScript() {
      return !isSource();
    }

  }

  @Value
  public static class SourceTableImport implements TableImport {

    @NonNull
    private final SourceTable table;
    @NonNull
    private final FlexibleDatasetSchema.TableField sourceSchema;
    @NonNull
    private final SchemaAdjustmentSettings schemaAdjustmentSettings;

    @Override
    public boolean isSource() {
      return true;
    }

    public InputTableSchema getSchema() {
      return new InputTableSchema(sourceSchema, table.hasSourceTimestamp());
    }
  }

  @Value
  public static class ScriptTableImport implements TableImport {

    @NonNull
    private final Name tableName;

    @Override
    public boolean isSource() {
      return false;
    }
    //TODO: to be filled out
  }

  private FlexibleDatasetSchema.TableField createTable(SourceTable table, Name datasetname,
      FlexibleDatasetSchema.TableField userSchema,
      SchemaAdjustmentSettings schemaAdjustmentSettings,
      ErrorCollector errors) {
    SourceTableStatistics stats = table.getStatistics();
    errors = errors.resolve(datasetname);
    if (userSchema == null) {
      if (stats.getCount() == 0) {
        errors.fatal(
            "We cannot infer schema for table [%s] due to lack of data. Need to provide user schema.",
            table.getName());
      }
      userSchema = FlexibleDatasetSchema.TableField.empty(table.getName());
    }
    SchemaGenerator generator = new SchemaGenerator(schemaAdjustmentSettings);
    FlexibleDatasetSchema.TableField result = generator.mergeSchema(stats, userSchema,
        errors.resolve(table.getName()));
    return result;
  }
}
