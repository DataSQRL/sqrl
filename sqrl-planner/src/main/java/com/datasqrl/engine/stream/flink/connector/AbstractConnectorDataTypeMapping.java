//package com.datasqrl.engine.stream.flink.connector;
//
//import com.datasqrl.calcite.type.TypeFactory;
//import com.datasqrl.config.TableConfig.ConnectorConfig;
//import com.datasqrl.config.TableConfig.Format;
//import com.datasqrl.engine.ExecutionEngine;
//import com.datasqrl.flink.FlinkConverter;
//import com.datasqrl.function.DowncastFunction;
//import com.datasqrl.functions.json.JsonDowncastFunction;
//import com.datasqrl.functions.vector.VectorDowncastFunction;
//import com.datasqrl.io.schema.json.FlexibleJsonFlinkFormatFactory;
//import com.datasqrl.json.FlinkJsonType;
//import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
//import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
//import com.datasqrl.plan.global.PhysicalDAGPlan.WriteSink;
//import com.datasqrl.vector.FlinkVectorType;
//import java.util.Optional;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.sql.SqlFunction;
//import org.apache.flink.table.functions.FunctionDefinition;
//import org.apache.flink.table.planner.plan.schema.RawRelDataType;
//
//public abstract class AbstractConnectorDataTypeMapping {
//
//  public void X() {
//
////
////      List<Class> conversionClasses = ServiceLoaderDiscovery.getAll(JdbcTypeSerializer.class).stream()
////          .map(JdbcTypeSerializer::getConversionClass)
////          .collect(Collectors.toList());
////
////      return conversionClasses.contains(relDataType.getRawType().getOriginatingClass());
//
//    FlinkConverter flinkConverter = new FlinkConverter(
//        (TypeFactory) framework.getQueryPlanner().getCatalogReader().getTypeFactory());
//
//    DowncastFunction downcastFunction = getEngineDowncastFunction(
//        getEngine(writeSink).get(), field.getType()).get();
//    //Otherwise apply downcasting
//
////      Class<?> defaultConversion = ((RawRelDataType) field.getType()).getRawType()
////          .getDefaultConversion();
//
////      DowncastFunction downcastFunction = ServiceLoaderDiscovery.get(DowncastFunction.class,
////          e -> e.getConversionClass().getName(), defaultConversion.getName());
//
//    String fncName = downcastFunction.downcastFunctionName().toLowerCase();
//    FunctionDefinition functionDef;
//    try {
//      functionDef = (FunctionDefinition) downcastFunction.getDowncastClassName()
//          .getDeclaredConstructor().newInstance();
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//
//    Optional<SqlFunction> convertedFunction = flinkConverter.convertFunction(fncName,
//        functionDef);
//
//  }
//
//
//  private Optional<ExecutionEngine> getEngine(WriteSink sink) {
//    if (sink instanceof EngineSink) {
//      return Optional.of(((EngineSink) sink).getStage().getEngine());
//    } else if (sink instanceof ExternalSink) {
//      return Optional.empty();
//    }
//    throw new RuntimeException("Unsupported sink");
//  }
//  /**
//   * Engines support different sets of data types. DataSQRL uses {@link DowncastFunction} to cast SQRL native types
//   * to the data type supported by the engine.
//   *
//   * @param type The type to cast
//   * @return The downcast function to use for the given type, or empty if no type casting is needed.
//   */
//  default Optional<DowncastFunction> getSinkTypeCastFunction(RelDataType type) {
//    // Convert sqrl native raw types to strings
//    if (type instanceof RawRelDataType) {
//      if ((((RawRelDataType)type).getRawType().getDefaultConversion() == FlinkJsonType.class)) {
//        return Optional.of(new JsonDowncastFunction());
//      } else if ((((RawRelDataType)type).getRawType().getDefaultConversion() == FlinkVectorType.class)) {
//        return Optional.of(new VectorDowncastFunction());
//      }
//    }
//
//    return Optional.empty(); //assume everything is supported by default
//  }
//
//  private boolean isRawType(RelDataType type) {
//    return type instanceof RawRelDataType;
//  }
//
//  private boolean formatSupportsType(ConnectorConfig tableConfig, RelDataType type) {
//    Optional<Format> format = tableConfig.getFormat();
//    if (format.isEmpty()) {
//      return false;
//    }
//
//    if (format.get().getName().equalsIgnoreCase(FlexibleJsonFlinkFormatFactory.FORMAT_NAME) && type instanceof RawRelDataType) {
//      RawRelDataType relDataType = (RawRelDataType) type;
//      return FlexibleJsonFlinkFormatFactory.getSupportedTypeClasses().contains(relDataType.getRawType().getOriginatingClass());
//    } else {
//      return !(type instanceof RawRelDataType);
//    }
//  }
//}
