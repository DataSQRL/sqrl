package com.datasqrl.plan.table;

import java.util.Optional;
import java.util.Set;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.sql.SqlCallRewriter;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
    if (tableSchema instanceof RelDataTypeTableSchema schema) {
      dataType = schema.getRelDataType();
    } else {
      dataType = SchemaToRelDataTypeFactory.load(tableSchema)
          .map(tableSchema, tableConfig, tableName, errors);
    }
    if (dataType==null) {
      throw errors.exception(ErrorCode.SCHEMA_ERROR, "Could not convert schema for table: %s", tableName);
    }

    var typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    var nameAdjuster = new NameAdjuster(dataType.getFieldNames());
    var planner = framework.getQueryPlanner();

    typeBuilder.addAll(dataType.getFieldList());

    var metadataConfig = tableConfig.getMetadataConfig();
    for (String columnName : metadataConfig.getKeys()) {
      if (nameAdjuster.contains(columnName)) {
		continue;
	}
      errors.checkFatal(!nameAdjuster.contains(columnName), "Metadata column name already used in data: %s", columnName);
      var colConfig = metadataConfig.getMetadataEntry(columnName)
          .get();

      var type = colConfig.getType();
      if (type.isPresent()) { // if has a type, use that, otherwise resolve as module
        if (!isValidDatatype(type.get())) {
          throw new RuntimeException(
              "Not a valid SQRL data type. Please check the documentation for supported SQRL types.");
        }
        var datatype = type.get();
        var metadataType = planner.getRelBuilder().getTypeFactory()
            .createTypeWithNullability(planner.parseDatatype(datatype), false);
        typeBuilder.add(nameAdjuster.uniquifyName(columnName), metadataType);
      } else if (colConfig.getAttribute().isPresent()){
        var attribute = colConfig.getAttribute().get();
        var sqlNode = framework.getQueryPlanner().parseCall(attribute);

        //Is a function call
        if (sqlNode instanceof SqlCall call) {
          var callRewriter = new SqlCallRewriter();
          callRewriter.performCallRewrite(call);

          addModules(framework, moduleLoader, errors, callRewriter.getFncModules());
          try {
            var rexNode = framework.getQueryPlanner()
                .planExpression(sqlNode, typeBuilder.build());

            typeBuilder.add(nameAdjuster.uniquifyName(columnName), rexNode.getType());
          } catch (Exception e) {
            throw new RuntimeException(
                "Could not evaluate metadata expression: %s. Reason: %s".formatted(attribute, e.getMessage()));
          }
        } else if (sqlNode instanceof SqlIdentifier identifier) {
          var relDataType = typeBuilder.build();

          RelDataTypeField field = relDataType.getField(attribute,
              false, false);
          if (field == null) {
            throw new RuntimeException("Could not find metadata field:" + identifier.getSimple());
          }
          typeBuilder.add(nameAdjuster.uniquifyName(columnName), field.getType());
        } else { //is a metadata column
          throw new RuntimeException("Could not derive type from metadata column: " + columnName);
        }
      } else {
        throw new RuntimeException("Unknown metadata column");
      }
    }


    var baseTblConfig = tableConfig.getBase();

    var finalType = typeBuilder.build();

    var primaryKeys = baseTblConfig
        .getPrimaryKey()
//        .validate(list -> list!=null && !list.isEmpty(), "Need to specify a primary key to unique identify records in table")
//        .validate(list -> list.stream().allMatch(nameAdjuster::contains),
//            String.format("Primary key column not found. Must be one of: %s", nameAdjuster))
        .get();

    var pkIndexes = new int[primaryKeys.size()];
    for (var i = 0; i < primaryKeys.size(); i++) {
      pkIndexes[i]=getFieldIndex(finalType, primaryKeys.get(i));
    }


    Preconditions.checkState(baseTblConfig.getTimestampColumn().isPresent(), "timestamp column missing");
    var timestampColumn = baseTblConfig.getTimestampColumn().get();
    if (!nameAdjuster.contains(timestampColumn)) {
      throw new RuntimeException("Timestamp column not found: \"%s\". Must be one of: %s".formatted(timestampColumn, nameAdjuster));
    }

    var tableType = tableConfig.getConnectorConfig().getTableType();

    return new SourceTableDefinition(finalType, new PrimaryKey(pkIndexes),
        baseTblConfig.getTimestampColumn().map(col -> getFieldIndex(finalType, col)), tableType);
  }

  private void addModules(SqrlFramework framework, ModuleLoader moduleLoader, ErrorCollector errors,
      Set<NamePath> fncModules) {
    for (NamePath attribute: fncModules) {
      var moduleOpt = moduleLoader.getModule(attribute.popLast());
      var name = attribute.getLast().getDisplay();
      var namespaceObject = moduleOpt.get()
          .getNamespaceObject(Name.system(name));
      namespaceObject.get().apply(null,Optional.empty(), framework, errors);
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
      log.info("Could not parse type: " + datatype + ".", e);
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
