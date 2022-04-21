package ai.datasqrl.graphql.execution.page;

import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Optional;

/**
 * Provides limit: Int
 */
public class NoPage implements PageProvider {

  @Override
  public Object wrap(List<Object> items, String page, boolean hasNextPage) {
    return items;
  }

  @Override
  public boolean hasNextPageAttribute(DataFetchingEnvironment environment) {
    return false;
  }

  @Override
  public Optional<Integer> parsePageSize(DataFetchingEnvironment environment) {
    Integer size = (Integer) environment.getArguments().get("limit");
    return Optional.ofNullable(size);
  }

  @Override
  public Optional<String> pageState(DataFetchingEnvironment environment) {
    return Optional.empty();
  }
}
