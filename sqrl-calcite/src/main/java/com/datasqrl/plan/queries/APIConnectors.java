package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.util.StreamUtil;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class APIConnectors {

  @Singular List<APIMutation> mutations;
  @Singular List<APISubscription> subscriptions;

  public Optional<APIMutation> getMutation(APISource source, Name name) {
    return getAPIConnector(mutations, source, name);
  }

  public Optional<APISubscription> getSubscription(APISource source, Name name) {
    return getAPIConnector(subscriptions, source, name);
  }

  private <T extends APIConnector> Optional<T> getAPIConnector(
      List<T> list, @NonNull APISource source, @NonNull Name name) {
    return StreamUtil.getOnlyElement(list.stream().filter(c -> equals(c, source, name)));
  }

  public static boolean equals(APIConnector c, APISource source, Name name) {
    return c.getName().equals(name) && c.getSource().equals(source);
  }
}
