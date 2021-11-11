package ai.dataeng.sqml.graphql;

import ai.dataeng.sqml.schema2.Field;
import lombok.Value;

@Value
public class ContextKey {
  String from;
  String to;
}
