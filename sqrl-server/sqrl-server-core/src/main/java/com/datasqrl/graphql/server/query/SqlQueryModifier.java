package com.datasqrl.graphql.server.query;

public interface SqlQueryModifier extends QueryModifier {

  record UserSqlQuery(Type type, String tableName, String parameterName, String tableSchema)
      implements SqlQueryModifier {

    @Override
    public boolean isPreprocessor() {
      return true;
    }

    public enum Type {
      FILTER,
      TRANSFORM
    }
  }
}
