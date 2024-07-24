//package com.datasqrl.engine.stream.flink.connector;
//
//import java.util.Optional;
//import org.apache.calcite.rel.type.RelDataType;
//
///**
// * Data type mapping for the sqrl-jdbc postgres connector
// */
//public class PostgresDataTypeMapping implements ConnectorDataTypeMapping {
//
//  @Override
//  public boolean supportsType(RelDataType type) {
//    return false;
//  }
//
//  @Override
//  public Optional<CastFunction> getDowncastFunction(RelDataType type) {
//    return Optional.empty();
//  }
//}
