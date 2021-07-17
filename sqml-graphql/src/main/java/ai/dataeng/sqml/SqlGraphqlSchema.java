//package ai.dataeng.sqml;
//
//import static graphql.Scalars.GraphQLString;
//
//import ai.dataeng.sqml.StubModel.ModelRelation;
//import ai.dataeng.sqml.analyzer.Analysis;
//import ai.dataeng.sqml.common.type.Type;
//import ai.dataeng.sqml.metadata.Metadata;
//import ai.dataeng.sqml.relation.VariableReferenceExpression;
//import graphql.GraphQL;
//import graphql.schema.GraphQLFieldDefinition;
//import graphql.schema.GraphQLList;
//import graphql.schema.GraphQLObjectType;
//import graphql.schema.GraphQLOutputType;
//import graphql.schema.GraphQLSchema;
//import graphql.schema.GraphQLSchema.Builder;
//import graphql.schema.GraphQLTypeReference;
//
//public class SqlGraphqlSchema {
//  private final Analysis analysis;
//  private final Metadata metadata; //todo remove
//
//  public SqlGraphqlSchema(Analysis analysis, Metadata metadata) {
//    this.analysis = analysis;
//    this.metadata = metadata;
//  }
//
//  public Builder build() {
//    GraphQLSchema.Builder schema = setupGraphQLJava(analysis.createStubModel(metadata));
//    return schema;
//  }
//
//  private static GraphQLSchema.Builder setupGraphQLJava(StubModel stubModel) {
//    GraphQLSchema.Builder schema = GraphQLSchema.newSchema();
//    for (ModelRelation relation : stubModel.relations) {
//      schema.additionalType(createType(relation));
//    }
//
//    //Todo: root retrievers
//    schema.query(GraphQLObjectType.newObject().name("Query")
//        .field(GraphQLFieldDefinition.newFieldDefinition()
//            .name("orders")
//            .type(GraphQLList.list(GraphQLTypeReference.typeRef("Orders")))
//            .build()).build());
//    return schema;
//  }
//
//  private static GraphQLOutputType createType(ModelRelation relation) {
//    GraphQLObjectType.Builder obj = GraphQLObjectType.newObject();
//    for (VariableReferenceExpression expr : relation.relation.getOutputVariables()) {
//      obj.field(createField(expr));
//    }
//    for (ModelRelation addl : relation.additionalRelations) {
//      obj.field(GraphQLFieldDefinition.newFieldDefinition()
//          .name(addl.name)
//          .type(createType(addl))
//          .build());
//    }
//
//    return obj.name(relation.name)
//        .build();
//  }
//
//  private static GraphQLFieldDefinition createField(VariableReferenceExpression expr) {
//    return GraphQLFieldDefinition.newFieldDefinition()
//        .name(expr.getName())
//        .type(asType(expr.getType()))
//        .build();
//  }
//
//  private static GraphQLOutputType asType(Type type) {
//    return GraphQLString;
//  }
//}