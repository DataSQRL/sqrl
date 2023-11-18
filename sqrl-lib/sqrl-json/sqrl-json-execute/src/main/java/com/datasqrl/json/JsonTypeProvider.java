//package com.datasqrl.json;
//
//import com.google.auto.service.AutoService;
//import org.apache.flink.table.functions.UserDefinedFunction;
//
//@AutoService(NativeType.class)
//public class JsonTypeProvider implements NativeType<UserDefinedFunction> {
//
//  @Override
//  public Class getType() {
//    return FlinkJsonType.class;
//  }
//
//  @Override
//  public UserDefinedFunction unknownTypeDowncast() {
//    return JsonFunctions.JSON_TO_STRING;
//  }
//
//  @Override
//  public String getPhysicalTypeName(String name) {
//    if (name.equalsIgnoreCase("calcite")) {
//      return "json";
//    } else if (name.equalsIgnoreCase("POSTGRES")) {
//      return "jsonb";
//    }
//    throw new RuntimeException("Could not get physical name of type");
//  }
//}
