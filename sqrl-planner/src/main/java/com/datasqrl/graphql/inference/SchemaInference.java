package com.datasqrl.graphql.inference;

import com.datasqrl.graphql.inference.SchemaInferenceModel.*;
import com.datasqrl.graphql.inference.argument.ArgumentHandler;
import com.datasqrl.graphql.inference.argument.EqHandler;
import com.datasqrl.graphql.inference.argument.LimitOffsetHandler;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.name.Name;
import com.datasqrl.schema.Column;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

@Getter
public class SchemaInference {

  private final TypeDefinitionRegistry registry;
  private final SqrlCalciteSchema schema;
  private final RootGraphqlModel.RootGraphqlModelBuilder root;
  List<ArgumentHandler> argumentHandlers = List.of(new EqHandler(), new LimitOffsetHandler());
  RelBuilder relBuilder;
  private Set<FieldDefinition> visited = new HashSet<>();
  private Map<ObjectTypeDefinition, SQRLTable> visitedObj = new HashMap<>();

  public SchemaInference(String gqlSchema, SqrlCalciteSchema schema, RelBuilder relBuilder) {
    this.registry = (new SchemaParser()).parse(gqlSchema);
    this.schema = schema;
    RootGraphqlModel.RootGraphqlModelBuilder root = RootGraphqlModel.builder()
        .schema(StringSchema.builder().schema(gqlSchema).build());
    this.root = root;
    this.relBuilder = relBuilder;
  }
  //Handles walking the schema completely

  public InferredSchema accept() {

    //resolve additional types
    resolveTypes();

    InferredQuery query = registry.getType("Query")
        .map(q -> resolveQueries((ObjectTypeDefinition) q))
        .orElseThrow(() -> new RuntimeException("Must have a query type"));

    Optional<InferredRootObject> mutation = registry.getType("Mutation")
        .map(m -> resolveMutations((ObjectTypeDefinition) m));

    Optional<InferredRootObject> subscription = registry.getType("subscription")
        .map(s -> resolveSubscriptions((ObjectTypeDefinition) s));

    InferredSchema inferredSchema = new InferredSchema(query, mutation, subscription);
    return inferredSchema;
  }

  private void resolveTypes() {
    //todo custom types: walk all defined types and import them
  }

  private InferredQuery resolveQueries(ObjectTypeDefinition query) {
    List<InferredField> fields = new ArrayList<>();
    for (FieldDefinition fieldDefinition : query.getFieldDefinitions()) {
      visited.add(fieldDefinition);
      fields.add(resolveQueryFromSchema(fieldDefinition, fields, query));
    }

    return new InferredQuery(query, fields);
  }

  private InferredField resolveQueryFromSchema(FieldDefinition fieldDefinition,
      List<InferredField> fields, ObjectTypeDefinition parent) {
    Optional<SQRLTable> sqrlTable = Optional.ofNullable(
            schema.getTable(fieldDefinition.getName(), false))
        .filter(t -> t.getTable() instanceof SQRLTable).map(t -> (SQRLTable) t.getTable());
    Preconditions.checkState(sqrlTable.isPresent(),
        "Could not find associated SQRL type for field {}", fieldDefinition.getName());
    SQRLTable table = sqrlTable.get();

    return inferObjectField(fieldDefinition, table, fields, parent);
  }

  private InferredField inferObjectField(FieldDefinition fieldDefinition, SQRLTable table,
      List<InferredField> fields, ObjectTypeDefinition parent) {
    TypeDefinition typeDef = unwrapObjectType(fieldDefinition.getType());

    if (typeDef instanceof ObjectTypeDefinition) {
      ObjectTypeDefinition obj = (ObjectTypeDefinition) typeDef;
      Preconditions.checkState(visitedObj.get(obj) == null || visitedObj.get(obj) == table,
          "Cannot redefine a type to point to a different SQRL table. Use an interface instead.");
      visitedObj.put(obj, table);
      InferredObjectField inferredObjectField = new InferredObjectField(parent, fieldDefinition,
          (ObjectTypeDefinition) typeDef, table);
      fields.addAll(walkChildren((ObjectTypeDefinition) typeDef, table, fields));
      return inferredObjectField;
    } else {
      throw new RuntimeException("Tbd");
    }
  }

  private List<InferredField> walkChildren(ObjectTypeDefinition typeDef, SQRLTable table,
      List<InferredField> fields) {
    Preconditions.checkState(!checkType(typeDef, table), "Field(s) not allowed [%s] in S5",
        getInvalidFields(typeDef, table), typeDef.getName());

    return typeDef.getFieldDefinitions().stream()
        .filter(f -> !visited.contains(f))
        .map(f -> walk(f, table.getField(Name.system(f.getName())).get(), fields, typeDef))
        .collect(Collectors.toList());
  }

  private InferredField walk(FieldDefinition fieldDefinition, Field field,
      List<InferredField> fields, ObjectTypeDefinition parent) {
    visited.add(fieldDefinition);
    if (field instanceof Relationship) {
      return walkRel(fieldDefinition, (Relationship) field, fields, parent);
    } else {
      return walkScalar(fieldDefinition, (Column) field, parent);
    }
  }

  private InferredField walkRel(FieldDefinition fieldDefinition, Relationship relationship,
      List<InferredField> fields, ObjectTypeDefinition parent) {
    return new NestedField(relationship,
        inferObjectField(fieldDefinition, relationship.getToTable(),
            fields, parent));
  }

  private InferredField walkScalar(FieldDefinition fieldDefinition, Column column,
      ObjectTypeDefinition parent) {
    /*
     * Check to see what types are compatible
     */
    return new InferredScalarField(fieldDefinition, column, parent);
  }

  private boolean checkType(ObjectTypeDefinition typeDef, SQRLTable table) {
    return typeDef.getFieldDefinitions().stream()
        .anyMatch(f -> table.getField(Name.system(f.getName())).isEmpty());
  }

  private List<String> getInvalidFields(ObjectTypeDefinition typeDef, SQRLTable table) {
    return typeDef.getFieldDefinitions().stream()
        .filter(f -> table.getField(Name.system(f.getName())).isEmpty())
        .map(FieldDefinition::getName)
        .collect(Collectors.toList());
  }

  private TypeDefinition unwrapObjectType(Type type) {
    //type can be in a single array with any non-nulls, e.g. [customer!]!
    type = unboxNonNull(type);
    if (type instanceof ListType) {
      type = ((ListType) type).getType();
    }
    type = unboxNonNull(type);

    Optional<TypeDefinition> typeDef = this.registry.getType(type);

    Preconditions.checkState(typeDef.isPresent(), "Could not find Object type");

    return typeDef.get();
  }

  private boolean isListType(Type type) {
    return unboxNonNull(type) instanceof ListType;
  }

  private Type unboxNonNull(Type type) {
    if (type instanceof NonNullType) {
      return unboxNonNull(((NonNullType) type).getType());
    }
    return type;
  }

  private InferredRootObject resolveMutations(ObjectTypeDefinition m) {
    return null;
  }

  private InferredRootObject resolveSubscriptions(ObjectTypeDefinition s) {

    return null;
  }
}
