/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.server.util;

import com.datasqrl.calcite.type.RelDataTypeWalker;
import com.datasqrl.calcite.type.TypeKind;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.planner.util.Documented.Lookup;
import com.datasqrl.server.graphql.CustomScalars;
import com.datasqrl.server.util.GraphqlSchemaUtil.GraphQLMetaType;
import graphql.Scalars;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * Maps a type to its GraphQL type. The mapped type is not wrapped in {@code NonNull}; the fields of
 * a row are.
 */
@RequiredArgsConstructor
public class GraphqlTypeWalker extends RelDataTypeWalker<GraphQLType> {

  private final GraphQLMetaType metaType;
  private final boolean extendedScalarTypes;

  /** The path of the type that the walk starts at */
  private final NamePath rootPath;

  /** The docs of the fields of the type that the walk starts at */
  private final Lookup rootDocs;

  @Override
  protected boolean includeField(RelDataTypeField field, NamePath fieldPath) {
    return !fieldPath.getLast().isHidden();
  }

  @Override
  protected Optional<GraphQLType> visitScalar(RelDataType type, TypeKind kind, NamePath path) {
    return Optional.ofNullable(
        switch (kind) {
          case BOOLEAN -> Scalars.GraphQLBoolean;
          case TINYINT, SMALLINT, INTEGER -> Scalars.GraphQLInt;
          case BIGINT -> extendedScalarTypes ? CustomScalars.LONG : Scalars.GraphQLFloat;
          case DECIMAL, FLOAT, DOUBLE -> Scalars.GraphQLFloat;
          case DATE -> CustomScalars.DATE;
          case TIME -> CustomScalars.TIME;
          case TIME_LTZ, TIMESTAMP, TIMESTAMP_LTZ -> CustomScalars.FLEXIBLE_DATETIME;
          case INTERVAL, STRING -> Scalars.GraphQLString;
          case JSON -> CustomScalars.JSON;
          default -> null;
        });
  }

  @Override
  protected Optional<GraphQLType> visitRow(
      RelDataType type, List<Field<GraphQLType>> fields, NamePath path) {
    var typeName = GraphqlSchemaUtil.uniquifyNameForPath(path, metaType.getSuffix());
    var fieldDocs = rootDocs.nested(path, rootPath.size());

    if (metaType == GraphQLMetaType.OUTPUT) {
      var builder = GraphQLObjectType.newObject().name(typeName);
      for (var field : fields) {
        var fieldBuilder =
            GraphQLFieldDefinition.newFieldDefinition()
                .name(field.field().getName())
                .type((GraphQLOutputType) wrapNullable(field));
        fieldDocs.getDocsFor(field.field().getName()).ifPresent(fieldBuilder::description);
        builder.field(fieldBuilder.build());
      }
      return Optional.of(builder.build());
    }

    var builder = GraphQLInputObjectType.newInputObject().name(typeName);
    for (var field : fields) {
      var fieldBuilder =
          GraphQLInputObjectField.newInputObjectField()
              .name(field.field().getName())
              .type((GraphQLInputType) wrapNullable(field));
      fieldDocs.getDocsFor(field.field().getName()).ifPresent(fieldBuilder::description);
      builder.field(fieldBuilder.build());
    }
    return Optional.of(builder.build());
  }

  @Override
  protected Optional<GraphQLType> visitArray(RelDataType type, GraphQLType element, NamePath path) {
    return Optional.of(GraphQLList.list(element));
  }

  @Override
  protected Optional<GraphQLType> visitMap(RelDataType type, NamePath path) {
    return Optional.of(CustomScalars.JSON);
  }

  private static GraphQLType wrapNullable(Field<GraphQLType> field) {
    return GraphqlSchemaUtil.wrapNullable(field.value(), field.field().getType());
  }
}
