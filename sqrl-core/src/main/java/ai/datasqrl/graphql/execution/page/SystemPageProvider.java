package ai.datasqrl.graphql.execution.page;

import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Provides filter: page-size: Int
 * PageInfo: data, pageInfo {cursor, hasNext}
 */
public class SystemPageProvider implements PageProvider {
  public Object wrap(List<Object> items, String page, boolean hasNextPage) {
    return Map.of("data", items, "pageInfo", Map.of("cursor", page, "hasNext",hasNextPage));
  }

  @Override
  public boolean hasNextPageAttribute(DataFetchingEnvironment environment) {
    return true;
  }

  @Override
  public Optional<Integer> parsePageSize(DataFetchingEnvironment environment) {
    Integer size = (Integer)environment.getArguments().get("page_size");

    return Optional.ofNullable(size).map(s->(s>0) ? s : 1);
  }

  @Override
  public Optional<String> pageState(DataFetchingEnvironment environment) {
    String state = (String)environment.getArguments().get("page_state");

    return Optional.ofNullable(state).map(s->{
        try {
          Integer.parseInt(s);
        } catch (Exception e) {
          return "0";
        }
        return s;
    });
  }
}
