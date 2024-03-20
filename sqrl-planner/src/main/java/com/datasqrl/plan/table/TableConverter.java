package com.datasqrl.plan.table;

import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.schema.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.sql.SqlCallRewriter;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqrlSqlValidator;

@AllArgsConstructor(onConstructor_=@Inject)
public class TableConverter {

  TypeFactory typeFactory;
  SqrlFramework framework;

  public SourceTableDefinition sourceToTable(
      TableSchema tableSchema, TableConfig tableConfig,
      Name tableName, ModuleLoader moduleLoader, ErrorCollector errors) {
    if (tableSchema.getLocation().isPresent()) {
      errors = errors.withConfig(tableSchema.getLocation().get());
    }
    RelDataType dataType;
    if (tableSchema instanceof RelDataTypeTableSchema) {
      dataType = ((RelDataTypeTableSchema) tableSchema).getRelDataType();
    } else {
      dataType = SchemaToRelDataTypeFactory.load(tableSchema)
          .map(tableSchema, tableName, errors);
    }
    if (dataType==null) {
      throw errors.exception(ErrorCode.SCHEMA_ERROR, "Could not convert schema for table: %s", tableName);
    }

    RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    NameAdjuster nameAdjuster = new NameAdjuster(dataType.getFieldNames());
    QueryPlanner planner = framework.getQueryPlanner();

    typeBuilder.addAll(dataType.getFieldList());

    SqrlConfig metadataConfig = tableConfig.getMetadataConfig();
    for (String columName : metadataConfig.getKeys()) {
      metadataConfig.getErrorCollector().checkFatal(!nameAdjuster.contains(columName), "Metadata column name already used in data: %s", columName);
      SqrlConfig colConfig = metadataConfig.getSubConfig(columName);

      SqrlConfig.Value<String> type = colConfig.asString(TableConfig.METADATA_COLUMN_TYPE_KEY);
      if (type.getOptional().isPresent()) { // if has a type, use that, otherwise resolve as module
        String datatype = colConfig.asString(TableConfig.METADATA_COLUMN_TYPE_KEY)
            .validate(this::isValidDatatype,
                "Not a valid SQRL data type. Please check the documentation for supported SQRL types.")
            .get();
        typeBuilder.add(nameAdjuster.uniquifyName(columName),
            planner.getRelBuilder().getTypeFactory()
                .createTypeWithNullability(planner.parseDatatype(datatype), false));
      } else if (colConfig.asString(TableConfig.METADATA_COLUMN_ATTRIBUTE_KEY).getOptional().isPresent()){
        String attribute = colConfig.asString(TableConfig.METADATA_COLUMN_ATTRIBUTE_KEY)
            .get();
        SqlNode sqlNode = framework.getQueryPlanner().parseCall(attribute);
        SqrlSqlValidator sqlValidator = (SqrlSqlValidator)framework.getQueryPlanner().createSqlValidator();

        //Is a function call
        if (sqlNode instanceof SqlCall) {
          SqlCallRewriter callRewriter = new SqlCallRewriter();
          callRewriter.performCallRewrite((SqlCall) sqlNode);

          addModules(framework, moduleLoader, errors, callRewriter.getFncModules());
          try {
            RelDataType relDataType1 = sqlValidator.inferReturnType(typeBuilder.build(),
                (SqlCall) sqlNode,
                framework.getCatalogReader());
            typeBuilder.add(nameAdjuster.uniquifyName(columName), relDataType1);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Could not evaluate metadata expression: %s. Reason: %s", attribute, e.getMessage()));
          }
        } else { //is a metadata column
          throw new RuntimeException("Could not derive type from metadata column: " + columName);
        }
      } else {
        throw new RuntimeException("Unknown metadata column");
      }
    }


    TableConfig.Base baseTblConfig = tableConfig.getBaseTableConfig();

    RelDataType finalType = typeBuilder.build();

    List<String> primaryKeys = baseTblConfig
        .getPrimaryKey()
//        .validate(list -> list!=null && !list.isEmpty(), "Need to specify a primary key to unique identify records in table")
//        .validate(list -> list.stream().allMatch(nameAdjuster::contains),
//            String.format("Primary key column not found. Must be one of: %s", nameAdjuster))
        .get();

    int[] pkIndexes = new int[primaryKeys.size()];
    for (int i = 0; i < primaryKeys.size(); i++) {
      pkIndexes[i]=getFieldIndex(finalType, primaryKeys.get(i));
    }


    Preconditions.checkState(baseTblConfig.getTimestampColumn().getOptional().isPresent(), "timestamp column missing");
    Optional<String> timestampCol = baseTblConfig.getTimestampColumn()
        .validate(nameAdjuster::contains, String.format("Column not found. Must be one of: %s", nameAdjuster)).getOptional();

    TableType tableType = tableConfig.getConnectorConfig().getTableType();

    return new SourceTableDefinition(finalType, new PrimaryKey(pkIndexes),
        timestampCol.map(col -> getFieldIndex(finalType, col)), tableType);
  }

  private void addModules(SqrlFramework framework, ModuleLoader moduleLoader, ErrorCollector errors,
      Set<NamePath> fncModules) {
    for (NamePath attribute: fncModules) {
      Optional<SqrlModule> moduleOpt = moduleLoader.getModule(attribute.popLast());
      String name = attribute.getLast().getDisplay();
      Optional<NamespaceObject> namespaceObject = moduleOpt.get()
          .getNamespaceObject(Name.system(name));
      namespaceObject.get().apply(Optional.empty(), framework, errors);
    }
  }

  private static int getFieldIndex(RelDataType type, String fieldName) {
    RelDataTypeField field = type.getField(fieldName,true, false);
    Preconditions.checkNotNull(field, "Could not find field: %s", fieldName);
    return field.getIndex();
  }

  private boolean isValidDatatype(String datatype) {
    try {
      framework.getQueryPlanner().parseDatatype(datatype);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Value
  public static class SourceTableDefinition {
    RelDataType dataType;
    PrimaryKey primaryKey;
    Optional<Integer> timestampIndex;
    TableType tableType;
  }


}
