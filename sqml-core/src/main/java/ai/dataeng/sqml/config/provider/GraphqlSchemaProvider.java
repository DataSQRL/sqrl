package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.api.graphql.GraphqlSchema;

public interface GraphqlSchemaProvider {
  GraphqlSchema getGraphqlSchema();
}
