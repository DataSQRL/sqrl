package com.datasqrl.plan.table;

import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import com.google.inject.Inject;
import java.util.LinkedHashMap;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

@AllArgsConstructor(onConstructor_=@Inject)
public class TableConverter {

  //TODO: Move to engine and look up via configured engine. For now, this is hard-coded for Flink
  public static Map<String,TableType> CONNECTOR_TYPE_MAP = ImmutableMap.of(
      "kafka",TableType.STREAM,
      "file", TableType.STREAM,
      "filesystem", TableType.STREAM,
      "upsert-kafka", TableType.VERSIONED_STATE,
      "jdbc", TableType.LOOKUP
      );

  TypeFactory typeFactory;
  SqrlFramework framework;


  public SourceTableDefinition sourceToTable(
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
    NameAdjuster nameAdjuster = new NameAdjuster(dataType.getFieldNames());

//    SqrlConfig metadataConfig = tableConfig.getMetadataConfig();
//    for (String columName : metadataConfig.getKeys()) {
//      SqrlConfig colConfig = metadataConfig.getSubConfig(columName);
//      String datatype = colConfig.asString(TableConfig.METADATA_COLUMN_TYPE_KEY).validate(this::isValidDatatype,
//          "Not a valid SQRL data type. Please check the documentation for supported SQRL types.").get();
//      typeBuilder.add(nameAdjuster.uniquifyName(columName), planner.parseDatatype(
//          datatype));
//    }

    //Remove the following in favor of the above
    typeBuilder.add(nameAdjuster.uniquifyName(ReservedName.UUID), TypeFactory.makeUuidType(typeFactory, false));
    typeBuilder.add(nameAdjuster.uniquifyName(ReservedName.INGEST_TIME), TypeFactory.makeTimestampType(typeFactory, false));
    if (tableConfig.getConnectorSettings().isHasSourceTimestamp()) {
      typeBuilder.add(nameAdjuster.uniquifyName(ReservedName.SOURCE_TIME), TypeFactory.makeTimestampType(typeFactory,false));
    }

    typeBuilder.addAll(dataType.getFieldList());
    RelDataType finalType = typeBuilder.build();

//    TableConfig.Base baseTblConfig = tableConfig.getBaseTableConfig();
//    List<String> primaryKeys = baseTblConfig
//        .getPrimaryKey()
//        .validate(list -> list!=null && !list.isEmpty(), "Need to specify a primary key to unique identify records in table")
//        .validate(list -> list.stream().allMatch(nameAdjuster::containsName),
//            String.format("Primary key column not found. Must be one of: %s", nameAdjuster))
//        .get();
//
//    int[] pkIndexes = new int[primaryKeys.size()];
//    for (int i = 0; i < primaryKeys.size(); i++) {
//      pkIndexes[i]=getFieldIndex(finalType, primaryKeys.get(i));
//    }

    //Remove the following in favor of the above
    int[] pkIndexes = new int[]{getFieldIndex(finalType, ReservedName.UUID.getDisplay())};

    Optional<String> timestampCol = Optional.empty();
//        baseTblConfig.getTimestampColumn()
//        .validate(nameAdjuster::containsName, String.format("Column not found. Must be one of: %s", nameAdjuster)).getOptional();

    //Look up table type
    TableType tableType = CONNECTOR_TYPE_MAP.get(tableConfig.getConnectorName().toLowerCase());
    if (tableType==null) tableType = TableType.STREAM;
//    Preconditions.checkArgument(tableType!=null, "Unrecognized connector: %s", tableConfig.getConnectorName());

    return new SourceTableDefinition(finalType, new PrimaryKey(pkIndexes), timestampCol.map(col -> getFieldIndex(finalType, col)), tableType);
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
