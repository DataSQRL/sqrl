package com.datasqrl.datatype;

import com.datasqrl.datatype.DataTypeMapping.SimpleMapper;
import com.datasqrl.json.JsonToString;
import com.datasqrl.json.ToJson;
import com.datasqrl.vector.DoubleToVector;
import com.datasqrl.vector.VectorToDouble;
import java.util.Optional;

public class DataTypeMappings {

  public static DataTypeMapping.Mapper JSON_STRING = new SimpleMapper(new JsonToString(), new ToJson());
  public static DataTypeMapping.Mapper JSON_TO_STRING_ONLY = new SimpleMapper(new JsonToString(), Optional.empty());
  public static DataTypeMapping.Mapper TO_JSON_ONLY = new SimpleMapper(new ToJson(), Optional.empty());
  public static DataTypeMapping.Mapper VECTOR_DOUBLE = new SimpleMapper(new VectorToDouble(), new DoubleToVector());
  public static DataTypeMapping.Mapper VECTOR_TO_DOUBLE_ONLY = new SimpleMapper(new VectorToDouble(), Optional.empty());
  public static DataTypeMapping.Mapper TO_BYTES_ONLY = new SimpleMapper(new SerializeToBytes(), Optional.empty());

}
