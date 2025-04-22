package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class APISubscription implements APIConnector {

  @EqualsAndHashCode.Include
  Name name;
  @EqualsAndHashCode.Include
  APISource source;
  //TODO: add required arguments for partitioning

}
