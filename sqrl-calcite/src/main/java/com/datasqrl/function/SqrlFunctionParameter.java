package com.datasqrl.function;

import static com.datasqrl.canonicalizer.ReservedName.VARIABLE_PREFIX;

import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.*;

import java.util.Optional;
import org.apache.calcite.sql.validate.SqlNameMatcher;

@Getter
public class SqrlFunctionParameter implements FunctionParameter {

  //This is the name of the argument:
  private final String name; //TODO: make sure the column name is properly resolved against the parent table: relDataType.getField(name, false, false)
  private final Optional<SqlNode> defaultValue; //TODO: remove
  private final SqlDataTypeSpec type; //TODO: This doesn't seem to be used - can we remove? Same as relDataType
  private final int ordinal; //the ordinal within the query
  private final RelDataType relDataType;  //this is the type of the argument
  private final boolean isInternal; //if true, this is a column on the "this" table, else a user provided argument
  private final ParameterName parentName; //TODO: remove; this is for case sensitiviy and name matching, could this just be the name of the parent field?

  public SqrlFunctionParameter(String name, Optional<SqlNode> defaultValue, SqlDataTypeSpec type,
      int ordinal, RelDataType relDataType, boolean isInternal, ParameterName parentName) {
    this.name = name;
    this.defaultValue = defaultValue;
    this.type = type;
    this.ordinal = ordinal;
    this.relDataType = relDataType;
    this.isInternal = isInternal;
    this.parentName = parentName;
  }

  @Override
  public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }

  @Override
  public boolean isOptional() {
    return defaultValue.isPresent();
  }

  public String getVariableName() {
    return name.charAt(0) == VARIABLE_PREFIX.getCanonical().charAt(0) ? name.substring(1) : name;
  }

  // Parameter names that we're not sure the correct name of
  public interface ParameterName {

    Optional<String> resolve(RelDataType parentType, SqlNameMatcher sqlNameMatcher);
  }

  // The casing is known to be correct, may not be the same name as the parameter name
  @Value
  public static class CasedParameter implements ParameterName {
    String name;

    @Override
    public Optional<String> resolve(RelDataType parentType, SqlNameMatcher sqlNameMatcher) {
      return Optional.of(name);
    }
  }

  // Used for cases where the user provided the name but it exists as a value with the same
  // name on its parent caller
  @Value
  public static class UnknownCaseParameter implements ParameterName {
    String name;

    @Override
    public Optional<String> resolve(RelDataType parentType, SqlNameMatcher sqlNameMatcher) {
      int index = sqlNameMatcher
          .indexOf(parentType.getFieldNames(), name);
      if (index == -1) {
        return Optional.empty();
      }
      RelDataTypeField field = parentType.getFieldList().get(index);
      return Optional.of(field.getName());
    }
  }

  @Value
  public static class NoParameter implements ParameterName {

    @Override
    public Optional<String> resolve(RelDataType parentType, SqlNameMatcher sqlNameMatcher) {
      return Optional.empty();
    }
  }
}
