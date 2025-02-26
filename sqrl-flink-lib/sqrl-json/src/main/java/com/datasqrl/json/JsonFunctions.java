package com.datasqrl.json;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies;

public class JsonFunctions {

  public static final ToJson TO_JSON = new ToJson();
  public static final JsonToString JSON_TO_STRING = new JsonToString();
  public static final JsonObject JSON_OBJECT = new JsonObject();
  public static final JsonArray JSON_ARRAY = new JsonArray();
  public static final JsonExtract JSON_EXTRACT = new JsonExtract();
  public static final JsonQuery JSON_QUERY = new JsonQuery();
  public static final JsonExists JSON_EXISTS = new JsonExists();
  public static final JsonArrayAgg JSON_ARRAYAGG = new JsonArrayAgg();
  public static final JsonObjectAgg JSON_OBJECTAGG = new JsonObjectAgg();
  public static final JsonConcat JSON_CONCAT = new JsonConcat();

  public static ArgumentTypeStrategy createJsonArgumentTypeStrategy(DataTypeFactory typeFactory) {
    return InputTypeStrategies.or(
        SpecificInputTypeStrategies.JSON_ARGUMENT,
        InputTypeStrategies.explicit(createJsonType(typeFactory)));
  }

  public static DataType createJsonType(DataTypeFactory typeFactory) {
    DataType dataType = DataTypes.of(FlinkJsonType.class).toDataType(typeFactory);
    return dataType;
  }
}
