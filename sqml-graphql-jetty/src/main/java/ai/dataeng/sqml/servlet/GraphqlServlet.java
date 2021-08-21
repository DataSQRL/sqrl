package ai.dataeng.sqml.servlet;


import ai.dataeng.sqml.dag.Dag;
import graphql.schema.GraphQLSchema;

public class GraphqlServlet {

  public static Builder newGraphqlServlet() {
    return new Builder();
  }

  public static class Builder {

    public Builder port(int port) {
      return this;
    }

    public Builder dag(Dag dag) {
      return this;
    }

    public Builder schema(GraphQLSchema graphqlSchema) {
      return this;
    }

    public GraphqlServlet build() {
      return new GraphqlServlet();
    }

  }
}
