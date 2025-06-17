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
package com.datasqrl.graphql.visitor;

import graphql.language.DirectiveDefinition;
import graphql.language.EnumTypeDefinition;
import graphql.language.EnumTypeExtensionDefinition;
import graphql.language.EnumValueDefinition;
import graphql.language.FragmentDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputObjectTypeExtensionDefinition;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.InterfaceTypeExtensionDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ObjectTypeExtensionDefinition;
import graphql.language.OperationDefinition;
import graphql.language.OperationTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.ScalarTypeExtensionDefinition;
import graphql.language.SchemaDefinition;
import graphql.language.SchemaExtensionDefinition;
import graphql.language.UnionTypeDefinition;
import graphql.language.UnionTypeExtensionDefinition;

public interface GraphqlDefinitionVisitor<R, C> {
  default R visitDirectiveDefinition(DirectiveDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitEnumTypeDefinition(EnumTypeDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitEnumTypeExtensionDefinition(EnumTypeExtensionDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitEnumValueDefinition(EnumValueDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitFragmentDefinition(FragmentDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitInputObjectTypeDefinition(InputObjectTypeDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitInputObjectTypeExtensionDefinition(
      InputObjectTypeExtensionDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitInterfaceTypeDefinition(InterfaceTypeDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitInterfaceTypeExtensionDefinition(
      InterfaceTypeExtensionDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitObjectTypeDefinition(ObjectTypeDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitObjectTypeExtensionDefinition(ObjectTypeExtensionDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitOperationDefinition(OperationDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitOperationTypeDefinition(OperationTypeDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitScalarTypeDefinition(ScalarTypeDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitScalarTypeExtensionDefinition(ScalarTypeExtensionDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitSchemaDefinition(SchemaDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitSchemaExtensionDefinition(SchemaExtensionDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitUnionTypeDefinition(UnionTypeDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }

  default R visitUnionTypeExtensionDefinition(UnionTypeExtensionDefinition node, C context) {
    throw new RuntimeException("Unknown node" + node);
  }
}
