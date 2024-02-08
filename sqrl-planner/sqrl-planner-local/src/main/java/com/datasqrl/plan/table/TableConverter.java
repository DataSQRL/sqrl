package com.datasqrl.plan.table;

import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

@AllArgsConstructor
public class TableConverter {

  public static String METADATA_COLUMN_TYPE_KEY = "type";
  public static String PRIMARYKEY_KEY = "primaryKey";


  TypeFactory typeFactory;
  QueryPlanner planner;


  public SourceTableType sourceToTable(
      TableSchema tableSchema, TableConfig tableConfig,
      Name tableName, ErrorCollector errors) {
    if (tableSchema.getLocation().isPresent()) {
      errors = errors.withConfig(tableSchema.getLocation().get());
    }
    RelDataType dataType = SchemaToRelDataTypeFactory.load(tableSchema)
        .map(tableSchema, tableName, errors);
    if (dataType==null) {
      throw errors.exception(ErrorCode.SCHEMA_ERROR, "Could not convert schema for table: %s", tableName);
    }

    RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    typeBuilder.addAll(dataType.getFieldList());
    NameAdjuster nameAdjuster = new NameAdjuster(dataType.getFieldNames());

    SqrlConfig metadataConfig = tableConfig.getMetadataConfig();
    for (String columName : metadataConfig.getKeys()) {
      SqrlConfig colConfig = metadataConfig.getSubConfig(columName);
      String datatype = colConfig.asString(METADATA_COLUMN_TYPE_KEY).validate(this::isValidDatatype,
          "Not a valid SQRL data type. Please check the documentation for supported SQRL types.").get();
      typeBuilder.add(nameAdjuster.uniquifyName(columName), planner.parseDatatype(
          datatype));
    }

    TableConfig.Base baseTblConfig = tableConfig.getBaseTableConfig();

    Optional<List<String>> primaryKeysOpt = baseTblConfig
        .getPrimaryKey().validate(list -> list.stream().allMatch(nameAdjuster::containsName),
            String.format("Primary key column not found. Must be one of: %s", nameAdjuster))
        .getOptional();

    List<String> primaryKeys;
    if (primaryKeysOpt.isEmpty()) {
      String pkName = nameAdjuster.uniquifyName(ReservedName.UUID);
      typeBuilder.add(pkName, TypeFactory.makeUuidType(typeFactory, false));
      primaryKeys = List.of(pkName);
    } else {
      primaryKeys = primaryKeysOpt.get();
    }
    RelDataType finalType = typeBuilder.build();

    int[] pkIndexes = new int[primaryKeys.size()];
    for (int i = 0; i < primaryKeys.size(); i++) {
      pkIndexes[i]=getFieldIndex(finalType, primaryKeys.get(i));
    }

    Optional<String> timestampCol = baseTblConfig.getTimestampColumn()
        .validate(nameAdjuster::containsName, String.format("Column not found. Must be one of: %s", nameAdjuster)).getOptional();

    return new SourceTableType(finalType, new PrimaryKey(pkIndexes), timestampCol.map(col -> getFieldIndex(finalType, col)));
  }

  private static int getFieldIndex(RelDataType type, String fieldName) {
    RelDataTypeField field = type.getField(fieldName,true, false);
    Preconditions.checkNotNull(field, "Could not find field: %s", fieldName);
    return field.getIndex();
  }

  private boolean isValidDatatype(String datatype) {
    try {
      planner.parseDatatype(datatype);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Value
  public static class SourceTableType {
    RelDataType type;
    PrimaryKey primaryKey;
    Optional<Integer> timestampIndex;
  }

  /**
   * NameAdjuster makes sure that any additional columns we add to a table (e.g. primary keys or timestamps) are unique and do
   * not clash with existing columns by `uniquifying` them using Calcite's standard way of doing this.
   * Because we want to preserve the names of the user-defined columns and primary key columns are added first, we have to use
   * this custom way of uniquifying column names.
   */
  public static class NameAdjuster {

    Set<String> names;

    public NameAdjuster(Collection<String> names) {
      this.names = new HashSet<>(names);
      Preconditions.checkArgument(this.names.size() == names.size(), "Duplicate names in set of columns: %s", names);
    }

    public String uniquifyName(Name name) {
      return uniquifyName(name.getDisplay());
    }

    public String uniquifyName(String name) {
      String uniqueName = SqlValidatorUtil.uniquify(
          name,
          names,
          SqlValidatorUtil.EXPR_SUGGESTER);
      names.add(uniqueName);
      return uniqueName;
    }

    @Override
    public String toString() {
      return names.toString();
    }

    public boolean containsName(String name) {
      return names.stream().anyMatch(name::equalsIgnoreCase);
    }

  }


}
