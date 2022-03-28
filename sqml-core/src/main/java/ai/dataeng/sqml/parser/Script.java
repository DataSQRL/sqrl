package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.schema.Namespace;
import graphql.schema.GraphQLCodeRegistry;
import lombok.ToString;
import lombok.Value;

@Value
@ToString
public class Script {
  Namespace namespace;
  GraphQLCodeRegistry registry;
}
