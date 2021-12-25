package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.api.graphql.GraphqlServlet;

public interface GraphqlServletProvider {
  GraphqlServlet getGraphqlServlet();
}
