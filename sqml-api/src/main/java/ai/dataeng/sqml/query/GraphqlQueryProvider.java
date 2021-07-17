package ai.dataeng.sqml.query;


import java.util.List;

public class GraphqlQueryProvider extends QueryProvider {

  public static Builder newQueryProvider() {
    return new Builder();
  }

  public static class Builder {

    public Builder query(String script, List<Query> queries) {
      return this;
    }

    public GraphqlQueryProvider build() {
      return new GraphqlQueryProvider();
    }
  }
}
