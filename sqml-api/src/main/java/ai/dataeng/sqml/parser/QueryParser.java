package ai.dataeng.sqml.parser;


import ai.dataeng.sqml.query.Query;
import java.util.List;

public class QueryParser {

  public static QueryParser newGraphqlParser() {
    return new QueryParser();
  }

  public List<Query> parse(String queries) {
    return List.of();
  }
}
