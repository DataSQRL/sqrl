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
package com.datasqrl.lsp;

import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.SymbolIndex.ApiFile;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FieldDefinition;
import graphql.language.FragmentDefinition;
import graphql.language.InlineFragment;
import graphql.language.ListType;
import graphql.language.Node;
import graphql.language.NodeTraverser;
import graphql.language.NodeVisitorStub;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.OperationDefinition;
import graphql.language.SchemaDefinition;
import graphql.language.SelectionSet;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.parser.Parser;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Finds where the GraphQL schemas and operations of a project name a table, function or
 * relationship of its scripts. The compiler matches the API with the scripts by name, ignoring
 * case: a table or function of the main script is a field of the query, mutation or subscription
 * type, and a relationship is a field of the type that is named after its table. The type of a
 * table or function, if it is named after it, is found as well, wherever it is used.
 */
class GraphqlNames {

  /**
   * The names to look for.
   *
   * @param rootField the root field of a table or function
   * @param type the type of a table or function
   * @param relationship the type of the table and the name of a relationship
   */
  record Names(
      Optional<String> rootField, Optional<String> type, Optional<TypeField> relationship) {

    static final Names NONE = new Names(Optional.empty(), Optional.empty(), Optional.empty());

    boolean isEmpty() {
      return rootField.isEmpty() && type.isEmpty() && relationship.isEmpty();
    }
  }

  /** A field of a type. */
  record TypeField(String type, String field) {}

  /**
   * What was found.
   *
   * @param ranges the names in the files
   * @param namedType the type that is named after the table or function, if a schema defines it
   * @param typeNames the names of all types that the schemas define
   * @param unreadable the files that could not be read or parsed
   */
  record Found(
      List<SourceRange> ranges,
      Optional<String> namedType,
      Set<String> typeNames,
      List<Path> unreadable) {

    boolean hasType(String name) {
      return typeNames.stream().anyMatch(name::equalsIgnoreCase);
    }
  }

  private record Parsed(Path file, ScriptText text, Document document) {}

  private final Names names;
  private final Map<String, String> rootTypes =
      new HashMap<>(
          Map.of("query", "Query", "mutation", "Mutation", "subscription", "Subscription"));
  private final Map<String, List<FieldDefinition>> fieldsByType = new HashMap<>();
  private final Set<String> typeNames = new LinkedHashSet<>();
  private final Set<SourceRange> ranges = new LinkedHashSet<>();
  private Optional<String> definedType = Optional.empty();

  private GraphqlNames(Names names) {
    this.names = names;
  }

  /**
   * Finds the names in the GraphQL files of a project.
   *
   * @param names what to look for
   * @param files the schemas and operations
   * @param read reads the current content of a file
   */
  static Found find(Names names, List<ApiFile> files, Function<Path, Optional<String>> read) {
    var finder = new GraphqlNames(names);
    var schemas = new ArrayList<Parsed>();
    var operations = new ArrayList<Parsed>();
    var unreadable = new ArrayList<Path>();
    for (var file : files) {
      var text = read.apply(file.file());
      try {
        if (text.isEmpty()) {
          unreadable.add(file.file());
          continue;
        }
        var parsed = new Parsed(file.file(), new ScriptText(text.get()), Parser.parse(text.get()));
        (file.schema() ? schemas : operations).add(parsed);
      } catch (RuntimeException e) {
        unreadable.add(file.file());
      }
    }
    schemas.forEach(parsed -> finder.addSchema(parsed.document()));
    finder.definedType =
        names
            .type()
            .flatMap(type -> finder.typeNames.stream().filter(type::equalsIgnoreCase).findFirst());
    schemas.forEach(finder::findNames);
    operations.forEach(finder::findNames);
    return new Found(
        List.copyOf(finder.ranges),
        finder.definedType,
        Set.copyOf(finder.typeNames),
        List.copyOf(unreadable));
  }

  private void addSchema(Document document) {
    for (var definition : document.getDefinitions()) {
      if (definition instanceof TypeDefinition<?> type) {
        typeNames.add(type.getName());
        if (type instanceof ObjectTypeDefinition object) {
          fieldsByType
              .computeIfAbsent(object.getName(), name -> new ArrayList<>())
              .addAll(object.getFieldDefinitions());
        }
      } else if (definition instanceof SchemaDefinition schema) {
        schema
            .getOperationTypeDefinitions()
            .forEach(
                operation ->
                    rootTypes.put(
                        operation.getName().toLowerCase(Locale.ROOT),
                        operation.getTypeName().getName()));
      }
    }
  }

  private void findNames(Parsed parsed) {
    for (var definition : parsed.document().getDefinitions()) {
      if (definition instanceof ObjectTypeDefinition object) {
        findInType(parsed, object);
      } else if (definition instanceof OperationDefinition operation) {
        var root = rootTypes.get(operation.getOperation().name().toLowerCase(Locale.ROOT));
        findInSelections(parsed, operation.getSelectionSet(), root);
      } else if (definition instanceof FragmentDefinition fragment) {
        var condition = fragment.getTypeCondition().getName();
        addTypeCondition(parsed, fragment, condition);
        findInSelections(parsed, fragment.getSelectionSet(), condition);
      }
    }
    definedType.ifPresent(
        type ->
            new NodeTraverser()
                .preOrder(
                    new NodeVisitorStub() {
                      @Override
                      public TraversalControl visitTypeName(
                          TypeName node, TraverserContext<Node> context) {
                        if (node.getName().equals(type)) {
                          add(parsed, node, node.getName(), false);
                        }
                        return TraversalControl.CONTINUE;
                      }
                    },
                    parsed.document()));
  }

  private void findInType(Parsed parsed, ObjectTypeDefinition object) {
    var name = object.getName();
    if (definedType.filter(name::equals).isPresent()) {
      add(parsed, object, name, false);
    }
    for (var field : object.getFieldDefinitions()) {
      if (matches(name, field.getName())) {
        add(parsed, field, field.getName(), false);
      }
    }
  }

  private void findInSelections(Parsed parsed, SelectionSet selections, String type) {
    if (selections == null) {
      return;
    }
    for (var selection : selections.getSelections()) {
      if (selection instanceof Field field) {
        if (type != null && matches(type, field.getName())) {
          add(parsed, field, field.getName(), field.getAlias() != null);
        }
        findInSelections(parsed, field.getSelectionSet(), fieldType(type, field.getName()));
      } else if (selection instanceof InlineFragment fragment) {
        var condition = fragment.getTypeCondition();
        if (condition != null) {
          addTypeCondition(parsed, fragment, condition.getName());
        }
        findInSelections(
            parsed, fragment.getSelectionSet(), condition == null ? type : condition.getName());
      }
    }
  }

  /** Whether a field of a type is one of the names to look for. */
  private boolean matches(String type, String field) {
    var isRootField =
        rootTypes.containsValue(type)
            && names.rootField().filter(field::equalsIgnoreCase).isPresent();
    var isRelationship =
        names
            .relationship()
            .filter(r -> r.type().equalsIgnoreCase(type) && r.field().equalsIgnoreCase(field))
            .isPresent();
    return isRootField || isRelationship;
  }

  /**
   * The name of the type of a field. Without a schema, the API is generated from the scripts, and
   * the type of a root field is named after its table.
   */
  private String fieldType(String type, String field) {
    if (type == null) {
      return null;
    }
    if (fieldsByType.isEmpty()) {
      return rootTypes.containsValue(type) ? field : null;
    }
    return fieldsByType.getOrDefault(type, List.of()).stream()
        .filter(definition -> definition.getName().equals(field))
        .findFirst()
        .map(definition -> typeName(definition.getType()))
        .orElse(null);
  }

  private static String typeName(Type<?> type) {
    if (type instanceof NonNullType nonNull) {
      return typeName(nonNull.getType());
    } else if (type instanceof ListType list) {
      return typeName(list.getType());
    }
    return ((TypeName) type).getName();
  }

  private void add(Parsed parsed, Node<?> node, String name, boolean aliased) {
    var location = node.getSourceLocation();
    if (location == null) {
      return;
    }
    var script = parsed.text();
    var from = script.offset(location.getLine() - 1, location.getColumn() - 1);
    nameToken(script.text(), from, name, aliased)
        .ifPresent(token -> addRange(parsed, token[0], token[1]));
  }

  /**
   * Adds the type condition of a fragment, e.g. {@code Orders} in {@code fragment F on Orders}, if
   * it is the type that is named after the table. The parser does not keep its location.
   */
  private void addTypeCondition(Parsed parsed, Node<?> fragment, String condition) {
    var location = fragment.getSourceLocation();
    if (location == null || definedType.filter(condition::equals).isEmpty()) {
      return;
    }
    var text = parsed.text().text();
    var from = parsed.text().offset(location.getLine() - 1, location.getColumn() - 1);
    var selections = text.indexOf('{', from);
    var matcher =
        Pattern.compile("\\bon\\s+(" + Pattern.quote(condition) + ")\\b")
            .matcher(text)
            .region(from, selections < 0 ? text.length() : selections);
    if (matcher.find()) {
      addRange(parsed, matcher.start(1), matcher.end(1));
    }
  }

  private void addRange(Parsed parsed, int start, int end) {
    var script = parsed.text();
    ranges.add(
        new SourceRange(
            parsed.file(),
            script.lineOf(start),
            script.characterOf(start),
            script.lineOf(end),
            script.characterOf(end)));
  }

  /**
   * Finds a name from the start of its node, which is before its description and keywords, e.g.
   * {@code "Doc" extend type Orders}, or before the alias of a field, e.g. {@code o: Orders}.
   *
   * @return the start and end offset of the name
   */
  static Optional<int[]> nameToken(String text, int from, String name, boolean aliased) {
    var i = from;
    var skipAlias = aliased;
    while (i < text.length()) {
      var c = text.charAt(i);
      if (Character.isWhitespace(c) || c == ',' || c == ':' || c == '﻿') {
        i++;
      } else if (c == '#') {
        var end = text.indexOf('\n', i);
        i = end < 0 ? text.length() : end;
      } else if (text.startsWith("\"\"\"", i)) {
        i = blockStringEnd(text, i + 3);
      } else if (c == '"') {
        i = stringEnd(text, i + 1);
      } else if (isNameStart(c)) {
        var start = i;
        while (i < text.length() && isNameChar(text.charAt(i))) {
          i++;
        }
        if (skipAlias) {
          skipAlias = false;
        } else if (text.substring(start, i).equals(name)) {
          return Optional.of(new int[] {start, i});
        }
        // Otherwise a keyword before the name, e.g. `type`
      } else {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  private static int blockStringEnd(String text, int from) {
    var i = from;
    while (i < text.length()) {
      if (text.startsWith("\\\"\"\"", i)) {
        i += 4;
      } else if (text.startsWith("\"\"\"", i)) {
        return i + 3;
      } else {
        i++;
      }
    }
    return text.length();
  }

  private static int stringEnd(String text, int from) {
    var i = from;
    while (i < text.length() && text.charAt(i) != '"' && text.charAt(i) != '\n') {
      i += text.charAt(i) == '\\' ? 2 : 1;
    }
    return Math.min(i + 1, text.length());
  }

  private static boolean isNameStart(char c) {
    return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
  }

  private static boolean isNameChar(char c) {
    return isNameStart(c) || (c >= '0' && c <= '9');
  }
}
