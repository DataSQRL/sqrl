package com.datasqrl.datatype;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.functions.FunctionDefinition;

import com.datasqrl.engine.stream.flink.connector.CastFunction;

import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * For mapping Flink types to and from database engine types
 */
@FunctionalInterface
public interface DataTypeMapping {

  enum Direction {TO_DATABASE, FROM_DATABASE}

  DataTypeMapping NONE = (type) -> Optional.empty();

  /**
   *
   * @param type The datatype to map
   * @return The {@link Mapper} for the given datatype or empty if no type mapping is needed.
   */
  Optional<Mapper> getMapper(RelDataType type);

  interface Mapper {

    default Optional<FunctionDefinition> getEngineMapping(Direction direction) {
      return switch (direction) {
	case TO_DATABASE -> Optional.of(toEngineMapping());
	case FROM_DATABASE -> fromEngineMapping();
	default -> throw new UnsupportedOperationException("Unrecognized direction: " + direction);
	};
    }

    /**
     * @return The {@link CastFunction} that maps the {@link RelDataType} to a supported engine type
     */
    FunctionDefinition toEngineMapping();

    /**
     * @return The {@link CastFunction} that maps back to a type an database engine internal type
     * that is identical or similar to the original {@link RelDataType}. Returns empty when no such mapping
     * is possible or needed.
     */
    Optional<FunctionDefinition> fromEngineMapping();
  }

  @Value
  @AllArgsConstructor
  class SimpleMapper implements Mapper {

	  FunctionDefinition toEngineMapping;
    Optional<FunctionDefinition> fromEngineMapping;

    public SimpleMapper(FunctionDefinition toEngineMapping, FunctionDefinition fromEngineMapping) {
      this(toEngineMapping, Optional.of(fromEngineMapping));
    }

    @Override
    public FunctionDefinition toEngineMapping() {
      return toEngineMapping;
    }

    @Override
    public Optional<FunctionDefinition> fromEngineMapping() {
      return fromEngineMapping;
    }

  }

}
