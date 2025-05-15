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

import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.BooleanValue;
import graphql.language.Definition;
import graphql.language.Directive;
import graphql.language.DirectiveDefinition;
import graphql.language.DirectiveLocation;
import graphql.language.Document;
import graphql.language.EnumTypeDefinition;
import graphql.language.EnumTypeExtensionDefinition;
import graphql.language.EnumValue;
import graphql.language.EnumValueDefinition;
import graphql.language.Field;
import graphql.language.FieldDefinition;
import graphql.language.FloatValue;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputObjectTypeExtensionDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.IntValue;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.InterfaceTypeExtensionDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.NullValue;
import graphql.language.ObjectField;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ObjectTypeExtensionDefinition;
import graphql.language.ObjectValue;
import graphql.language.OperationDefinition;
import graphql.language.OperationTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.ScalarTypeExtensionDefinition;
import graphql.language.SchemaDefinition;
import graphql.language.SchemaExtensionDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.language.StringValue;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.language.UnionTypeDefinition;
import graphql.language.UnionTypeExtensionDefinition;
import graphql.language.Value;
import graphql.language.VariableDefinition;
import graphql.language.VariableReference;

// Todo break up into multiple per tree
public abstract class GraphqlSchemaVisitor<R, C>
    implements GraphqlArgumentVisitor<R, C>,
        GraphqlDefinitionVisitor<R, C>,
        GraphqlDirectiveLocationVisitor<R, C>,
        GraphqlDirectiveVisitor<R, C>,
        GraphqlDocumentVisitor<R, C>,
        GraphqlFieldDefinitionVisitor<R, C>,
        GraphqlInputValueDefinitionVisitor<R, C>,
        GraphqlObjectFieldVisitor<R, C>,
        GraphqlSelectionSetVisitor<R, C>,
        GraphqlSelectionVisitor<R, C>,
        GraphqlTypeVisitor<R, C>,
        GraphqlValueVisitor<R, C>,
        GraphqlVariableDefinitionVisitor<R, C> {

  public static <R, C> R accept(GraphqlDirectiveVisitor<R, C> visitor, Directive node, C context) {
    if (node instanceof Directive) {
      return visitor.visitDirective(node, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(GraphqlArgumentVisitor<R, C> visitor, Argument node, C context) {
    if (node instanceof Argument) {
      return visitor.visitArgument(node, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(
      GraphqlDefinitionVisitor<R, C> visitor, Definition node, C context) {
    if (node instanceof DirectiveDefinition definition) {
      return visitor.visitDirectiveDefinition(definition, context);
    } else if (node instanceof EnumTypeExtensionDefinition definition) {
      return visitor.visitEnumTypeExtensionDefinition(definition, context);
    } else if (node instanceof EnumTypeDefinition definition) {
      return visitor.visitEnumTypeDefinition(definition, context);
    } else if (node instanceof EnumValueDefinition definition) {
      return visitor.visitEnumValueDefinition(definition, context);
    } else if (node instanceof FragmentDefinition definition) {
      return visitor.visitFragmentDefinition(definition, context);
    } else if (node instanceof InputObjectTypeExtensionDefinition definition) {
      return visitor.visitInputObjectTypeExtensionDefinition(definition, context);
    } else if (node instanceof InputObjectTypeDefinition definition) {
      return visitor.visitInputObjectTypeDefinition(definition, context);
    } else if (node instanceof InterfaceTypeExtensionDefinition definition) {
      return visitor.visitInterfaceTypeExtensionDefinition(definition, context);
    } else if (node instanceof InterfaceTypeDefinition definition) {
      return visitor.visitInterfaceTypeDefinition(definition, context);
    } else if (node instanceof ObjectTypeExtensionDefinition definition) {
      return visitor.visitObjectTypeExtensionDefinition(definition, context);
    } else if (node instanceof ObjectTypeDefinition definition) {
      return visitor.visitObjectTypeDefinition(definition, context);
    } else if (node instanceof OperationDefinition definition) {
      return visitor.visitOperationDefinition(definition, context);
    } else if (node instanceof OperationTypeDefinition definition) {
      return visitor.visitOperationTypeDefinition(definition, context);
    } else if (node instanceof ScalarTypeExtensionDefinition definition) {
      return visitor.visitScalarTypeExtensionDefinition(definition, context);
    } else if (node instanceof ScalarTypeDefinition definition) {
      return visitor.visitScalarTypeDefinition(definition, context);
    } else if (node instanceof SchemaExtensionDefinition definition) {
      return visitor.visitSchemaExtensionDefinition(definition, context);
    } else if (node instanceof SchemaDefinition definition) {
      return visitor.visitSchemaDefinition(definition, context);
    } else if (node instanceof UnionTypeExtensionDefinition definition) {
      return visitor.visitUnionTypeExtensionDefinition(definition, context);
    } else if (node instanceof UnionTypeDefinition definition) {
      return visitor.visitUnionTypeDefinition(definition, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(GraphqlDocumentVisitor<R, C> visitor, Document node, C context) {

    if (node instanceof Document) {
      return visitor.visitDocument(node, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(
      GraphqlFieldDefinitionVisitor<R, C> visitor, FieldDefinition node, C context) {
    if (node instanceof FieldDefinition) {
      return visitor.visitFieldDefinition(node, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(
      GraphqlInputValueDefinitionVisitor<R, C> visitor, InputValueDefinition node, C context) {
    if (node instanceof InputValueDefinition) {
      return visitor.visitInputValueDefinition(node, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(
      GraphqlObjectFieldVisitor<R, C> visitor, ObjectField node, C context) {
    if (node instanceof ObjectField) {
      return visitor.visitObjectField(node, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(
      GraphqlSelectionSetVisitor<R, C> visitor, SelectionSet node, C context) {
    if (node instanceof SelectionSet) {
      return visitor.visitSelectionSet(node, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(GraphqlSelectionVisitor<R, C> visitor, Selection node, C context) {
    if (node instanceof Field field) {
      return visitor.visitField(field, context);
    } else if (node instanceof FragmentSpread spread) {
      return visitor.visitFragmentSpread(spread, context);
    } else if (node instanceof InlineFragment fragment) {
      return visitor.visitInlineFragment(fragment, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(GraphqlTypeVisitor<R, C> visitor, Type node, C context) {
    if (node instanceof ListType type) {
      return visitor.visitListType(type, context);
    } else if (node instanceof NonNullType type) {
      return visitor.visitNonNullType(type, context);
    } else if (node instanceof TypeName name) {
      return visitor.visitTypeName(name, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(GraphqlValueVisitor<R, C> visitor, Value node, C context) {
    if (node instanceof ArrayValue value) {
      return visitor.visitArrayValue(value, context);
    } else if (node instanceof BooleanValue value) {
      return visitor.visitBooleanValue(value, context);
    } else if (node instanceof EnumValue value) {
      return visitor.visitEnumValue(value, context);
    } else if (node instanceof FloatValue value) {
      return visitor.visitFloatValue(value, context);
    } else if (node instanceof IntValue value) {
      return visitor.visitIntValue(value, context);
    } else if (node instanceof NullValue value) {
      return visitor.visitNullValue(value, context);
    } else if (node instanceof ObjectValue value) {
      return visitor.visitObjectValue(value, context);
    } else if (node instanceof StringValue value) {
      return visitor.visitStringValue(value, context);
    } else if (node instanceof VariableReference reference) {
      return visitor.visitVariableReference(reference, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  public static <R, C> R accept(
      GraphqlVariableDefinitionVisitor<R, C> visitor, VariableDefinition node, C context) {
    if (node instanceof VariableDefinition) {
      return visitor.visitVariableDefinition(node, context);
    }
    throw new RuntimeException("Unknown graphql node");
  }

  //
  //  public static <R, C> R accept(GraphqlSchemaVisitor<R, C> visitor, Node node, C context) {
  //    if (node instanceof Argument) {
  //      return visitor.visitArgument((Argument) node, context);
  //    } else if (node instanceof ArrayValue) {
  //      return visitor.visitArrayValue((ArrayValue) node, context);
  //    } else if (node instanceof BooleanValue) {
  //      return visitor.visitBooleanValue((BooleanValue) node, context);
  //    } else if (node instanceof Directive) {
  //      return visitor.visitDirective((Directive) node, context);
  //    } else if (node instanceof DirectiveDefinition) {
  //      return visitor.visitDirectiveDefinition((DirectiveDefinition) node, context);
  //    } else if (node instanceof DirectiveLocation) {
  //      return visitor.visitDirectiveLocation((DirectiveLocation) node, context);
  //    } else if (node instanceof Document) {
  //      return visitor.visitDocument((Document) node, context);
  //    } else if (node instanceof EnumTypeExtensionDefinition) {
  //      return visitor.visitEnumTypeExtensionDefinition((EnumTypeExtensionDefinition) node,
  // context);
  //    } else if (node instanceof EnumTypeDefinition) {
  //      return visitor.visitEnumTypeDefinition((EnumTypeDefinition) node, context);
  //    } else if (node instanceof EnumValue) {
  //      return visitor.visitEnumValue((EnumValue) node, context);
  //    } else if (node instanceof EnumValueDefinition) {
  //      return visitor.visitEnumValueDefinition((EnumValueDefinition) node, context);
  //    } else if (node instanceof Field) {
  //      return visitor.visitField((Field) node, context);
  //    } else if (node instanceof FieldDefinition) {
  //      return visitor.visitFieldDefinition((FieldDefinition) node, context);
  //    } else if (node instanceof FloatValue) {
  //      return visitor.visitFloatValue((FloatValue) node, context);
  //    } else if (node instanceof FragmentDefinition) {
  //      return visitor.visitFragmentDefinition((FragmentDefinition) node, context);
  //    } else if (node instanceof FragmentSpread) {
  //      return visitor.visitFragmentSpread((FragmentSpread) node, context);
  //    } else if (node instanceof InlineFragment) {
  //      return visitor.visitInlineFragment((InlineFragment) node, context);
  //    } else if (node instanceof InputObjectTypeExtensionDefinition) {
  //      return visitor.visitInputObjectTypeExtensionDefinition(
  //          (InputObjectTypeExtensionDefinition) node, context);
  //    } else if (node instanceof InputObjectTypeDefinition) {
  //      return visitor.visitInputObjectTypeDefinition((InputObjectTypeDefinition) node, context);
  //    } else if (node instanceof InputValueDefinition) {
  //      return visitor.visitInputValueDefinition((InputValueDefinition) node, context);
  //    } else if (node instanceof IntValue) {
  //      return visitor.visitIntValue((IntValue) node, context);
  //    } else if (node instanceof InterfaceTypeExtensionDefinition) {
  //      return visitor.visitInterfaceTypeExtensionDefinition((InterfaceTypeExtensionDefinition)
  // node,
  //          context);
  //    } else if (node instanceof InterfaceTypeDefinition) {
  //      return visitor.visitInterfaceTypeDefinition((InterfaceTypeDefinition) node, context);
  //    } else if (node instanceof ListType) {
  //      return visitor.visitListType((ListType) node, context);
  //    } else if (node instanceof NonNullType) {
  //      return visitor.visitNonNullType((NonNullType) node, context);
  //    } else if (node instanceof NullValue) {
  //      return visitor.visitNullValue((NullValue) node, context);
  //    } else if (node instanceof ObjectField) {
  //      return visitor.visitObjectField((ObjectField) node, context);
  //    } else if (node instanceof ObjectTypeExtensionDefinition) {
  //      return visitor.visitObjectTypeExtensionDefinition((ObjectTypeExtensionDefinition) node,
  //          context);
  //    } else if (node instanceof ObjectTypeDefinition) {
  //      return visitor.visitObjectTypeDefinition((ObjectTypeDefinition) node, context);
  //    } else if (node instanceof ObjectValue) {
  //      return visitor.visitObjectValue((ObjectValue) node, context);
  //    } else if (node instanceof OperationDefinition) {
  //      return visitor.visitOperationDefinition((OperationDefinition) node, context);
  //    } else if (node instanceof OperationTypeDefinition) {
  //      return visitor.visitOperationTypeDefinition((OperationTypeDefinition) node, context);
  //    } else if (node instanceof ScalarTypeExtensionDefinition) {
  //      return visitor.visitScalarTypeExtensionDefinition((ScalarTypeExtensionDefinition) node,
  //          context);
  //    } else if (node instanceof ScalarTypeDefinition) {
  //      return visitor.visitScalarTypeDefinition((ScalarTypeDefinition) node, context);
  //    } else if (node instanceof SchemaExtensionDefinition) {
  //      return visitor.visitSchemaExtensionDefinition((SchemaExtensionDefinition) node, context);
  //    } else if (node instanceof SchemaDefinition) {
  //      return visitor.visitSchemaDefinition((SchemaDefinition) node, context);
  //    } else if (node instanceof SelectionSet) {
  //      return visitor.visitSelectionSet((SelectionSet) node, context);
  //    } else if (node instanceof StringValue) {
  //      return visitor.visitStringValue((StringValue) node, context);
  //    } else if (node instanceof TypeName) {
  //      return visitor.visitTypeName((TypeName) node, context);
  //    } else if (node instanceof UnionTypeExtensionDefinition) {
  //      return visitor.visitUnionTypeExtensionDefinition((UnionTypeExtensionDefinition) node,
  //          context);
  //    } else if (node instanceof UnionTypeDefinition) {
  //      return visitor.visitUnionTypeDefinition((UnionTypeDefinition) node, context);
  //    } else if (node instanceof VariableDefinition) {
  //      return visitor.visitVariableDefinition((VariableDefinition) node, context);
  //    } else if (node instanceof VariableReference) {
  //      return visitor.visitVariableReference((VariableReference) node, context);
  //    }
  //    throw new RuntimeException("Unknown graphql node");
  //  }

  @Override
  public R visitArgument(Argument node, C context) {
    return null;
  }

  @Override
  public R visitArrayValue(ArrayValue node, C context) {
    return null;
  }

  @Override
  public R visitBooleanValue(BooleanValue node, C context) {
    return null;
  }

  @Override
  public R visitDirective(Directive node, C context) {
    return null;
  }

  @Override
  public R visitDirectiveDefinition(DirectiveDefinition node, C context) {
    return null;
  }

  @Override
  public R visitDirectiveLocation(DirectiveLocation node, C context) {
    return null;
  }

  @Override
  public R visitDocument(Document node, C context) {
    return null;
  }

  @Override
  public R visitEnumTypeDefinition(EnumTypeDefinition node, C context) {
    return null;
  }

  @Override
  public R visitEnumTypeExtensionDefinition(EnumTypeExtensionDefinition node, C context) {
    return null;
  }

  @Override
  public R visitEnumValue(EnumValue node, C context) {
    return null;
  }

  @Override
  public R visitEnumValueDefinition(EnumValueDefinition node, C context) {
    return null;
  }

  @Override
  public R visitField(Field node, C context) {
    return null;
  }

  @Override
  public R visitFieldDefinition(FieldDefinition node, C context) {
    return null;
  }

  @Override
  public R visitFloatValue(FloatValue node, C context) {
    return null;
  }

  @Override
  public R visitFragmentDefinition(FragmentDefinition node, C context) {
    return null;
  }

  @Override
  public R visitFragmentSpread(FragmentSpread node, C context) {
    return null;
  }

  @Override
  public R visitInlineFragment(InlineFragment node, C context) {
    return null;
  }

  @Override
  public R visitInputObjectTypeDefinition(InputObjectTypeDefinition node, C context) {
    return null;
  }

  @Override
  public R visitInputObjectTypeExtensionDefinition(
      InputObjectTypeExtensionDefinition node, C context) {
    return null;
  }

  @Override
  public R visitInputValueDefinition(InputValueDefinition node, C context) {
    return null;
  }

  @Override
  public R visitIntValue(IntValue node, C context) {
    return null;
  }

  @Override
  public R visitInterfaceTypeDefinition(InterfaceTypeDefinition node, C context) {
    return null;
  }

  @Override
  public R visitInterfaceTypeExtensionDefinition(InterfaceTypeExtensionDefinition node, C context) {
    return null;
  }

  @Override
  public R visitListType(ListType node, C context) {
    return null;
  }

  @Override
  public R visitNonNullType(NonNullType node, C context) {
    return null;
  }

  @Override
  public R visitNullValue(NullValue node, C context) {
    return null;
  }

  @Override
  public R visitObjectField(ObjectField node, C context) {
    return null;
  }

  @Override
  public R visitObjectTypeDefinition(ObjectTypeDefinition node, C context) {
    return null;
  }

  @Override
  public R visitObjectTypeExtensionDefinition(ObjectTypeExtensionDefinition node, C context) {
    return null;
  }

  @Override
  public R visitObjectValue(ObjectValue node, C context) {
    return null;
  }

  @Override
  public R visitOperationDefinition(OperationDefinition node, C context) {
    return null;
  }

  @Override
  public R visitOperationTypeDefinition(OperationTypeDefinition node, C context) {
    return null;
  }

  @Override
  public R visitScalarTypeDefinition(ScalarTypeDefinition node, C context) {
    return null;
  }

  @Override
  public R visitScalarTypeExtensionDefinition(ScalarTypeExtensionDefinition node, C context) {
    return null;
  }

  @Override
  public R visitSchemaDefinition(SchemaDefinition node, C context) {
    return null;
  }

  @Override
  public R visitSchemaExtensionDefinition(SchemaExtensionDefinition node, C context) {
    return null;
  }

  @Override
  public R visitSelectionSet(SelectionSet node, C context) {
    return null;
  }

  @Override
  public R visitStringValue(StringValue node, C context) {
    return null;
  }

  @Override
  public R visitTypeName(TypeName node, C context) {
    return null;
  }

  @Override
  public R visitUnionTypeDefinition(UnionTypeDefinition node, C context) {
    return null;
  }

  @Override
  public R visitUnionTypeExtensionDefinition(UnionTypeExtensionDefinition node, C context) {
    return null;
  }

  @Override
  public R visitVariableDefinition(VariableDefinition node, C context) {
    return null;
  }

  @Override
  public R visitVariableReference(VariableReference node, C context) {
    return null;
  }
}
