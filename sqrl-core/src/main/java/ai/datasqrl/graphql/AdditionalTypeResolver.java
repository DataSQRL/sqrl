package ai.datasqrl.graphql;

import com.google.common.collect.Maps;
import graphql.language.ScalarTypeDefinition;
import graphql.schema.Coercing;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.idl.ScalarInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class AdditionalTypeResolver {
//  Map<String, GraphQLScalarType> builtInTypes =
//      Maps.uniqueIndex(ScalarInfo.GRAPHQL_SPECIFICATION_SCALARS,
//          GraphQLScalarType::getName);

  Map<String, GraphQLScalarType> resolvedTypes = new HashMap<>();

  public AdditionalTypeResolver() {
    resolvedTypes.put("UUID", GraphQLScalarType.newScalar()
        .name("UUID")
        .description("A custom scalar that handles emails")
        .coercing(new Coercing() {
          @Override
          public Object serialize(Object dataFetcherResult) {
//            return serializeEmail(dataFetcherResult);
            return null;
          }

          @Override
          public Object parseValue(Object input) {
//            return parseEmailFromVariable(input);
            return null;
          }

          @Override
          public Object parseLiteral(Object input) {
//            return parseEmailFromAstLiteral(input);
            return null;
          }
        })
        .build());

  }

  public GraphQLScalarType resolve(ScalarTypeDefinition type) {
//    Optional<GraphQLScalarType> t = Optional.ofNullable(builtInTypes.get(type.getName()));

    return resolvedTypes.get(type.getName());
  }

  //find jars
}
