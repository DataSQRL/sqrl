/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.plan.global.DAGAssembler;
import com.datasqrl.plan.global.DAGBuilder;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.DAGPreparation;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelector;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.QueryIndexSummary;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.util.SqlNameUtil;
import com.datasqrl.plan.queries.APISourceImpl;
import com.datasqrl.util.TestScript;
import graphql.schema.idl.SchemaParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;

public class AbstractSchemaInferenceModelTest extends AbstractLogicalSQRLIT {

  @SneakyThrows

//    String schemaStr = Files.readString(schemaPath);
//    plan(script.getScript());
//    Triple<Object, RootGraphqlModel, APIConnectorManager> result = inferSchemaModelQueries(
//        schemaStr, framework, errors);
//    return Pair.of(result.getLeft(), result.getRight());
//  }
//
  public void inferSchemaModelQueries(
      String schemaStr, SqrlFramework framework, ErrorCollector errors) {
    GraphQLMutationExtraction mutationExtraction = injector.getInstance(
        GraphQLMutationExtraction.class);

    mutationExtraction.analyze(new APISourceImpl(Name.system("schema"), schemaStr));

    GraphqlSchemaValidator validator = injector.getInstance(GraphqlSchemaValidator.class);
    APISourceImpl source = new APISourceImpl(Name.system("schema"), schemaStr);
    validator.validate(source, errors);

    APIConnectorManager apiManager = injector.getInstance(APIConnectorManager.class);
    GraphqlQueryGenerator queryGenerator = new GraphqlQueryGenerator(framework.getCatalogReader().nameMatcher(),
        framework.getSchema(),
        new GraphqlQueryBuilder(framework, apiManager,
            new SqlNameUtil(NameCanonicalizer.SYSTEM)), apiManager);

    queryGenerator.walk(source);
    queryGenerator.getQueries().forEach(apiManager::addQuery);
    queryGenerator.getSubscriptions().forEach(s->apiManager.addSubscription(
        new APISubscription(s.getAbsolutePath().getFirst(), source), s));
  }
//    APISource source = APISourceImpl.of(schemaStr);
//    //Inference
////    GraphQLMutationExtraction preAnalysis = new GraphQLMutationExtraction(
////        framework.getTypeFactory(),
////        NameCanonicalizer.SYSTEM);
//
//    APIConnectorManager apiManager = this.injector.getInstance(APIConnectorManager.class);
//=======
  @SneakyThrows
  public void inferSchemaAndQueries(TestScript script,
      Path schemaPath) {
    plan(script.getScript());
    inferSchemaModelQueries(Files.readString(schemaPath), framework,
        errors);

  }
//    String schemaStr = Files.readString(schemaPath);
//    plan(script.getScript());
//    return inferSchemaModelQueries(schemaStr, framework, errors)
//        .getLeft();
//  }
//
//  public Pair<InferredSchema, RootGraphqlModel> inferSchemaModelQueries(String schemaStr,
//      SqrlFramework framework, ErrorCollector errors) {
//    APISource source = APISourceImpl.of(schemaStr);
//    //Inference
//    SqrlSchemaForInference sqrlSchemaForInference = new SqrlSchemaForInference(
//        framework.getSchema());
//
//    APIConnectorManager apiManager = injector.getInstance(APIConnectorManager.class);
//    SchemaInference inference = new SchemaInference(framework, "<schema>",
//        apiManager.getModuleLoader(), source, sqrlSchemaForInference,
//        framework.getQueryPlanner().getRelBuilder(), apiManager);
//
//    InferredSchema inferredSchema;
//>>>>>>> 8622af179 (Reuse more code)
//    try {
      //todo readd once moved
//      preAnalysis.analyze(source);
//
//      GraphqlSchemaValidator schemaValidator = new GraphqlSchemaValidator(
//          framework.getCatalogReader().nameMatcher(),
//          framework.getSchema(), source, (new SchemaParser()).parse(source.getSchemaDefinition()),
//          apiManager);
//      schemaValidator.validate(source, errors);
//
//      GraphqlQueryGenerator queryGenerator = new GraphqlQueryGenerator(framework.getCatalogReader().nameMatcher(),
//          framework.getSchema(),  (new SchemaParser()).parse(source.getSchemaDefinition()), source,
//          new GraphqlQueryBuilder(framework, apiManager, new SqlNameUtil(NameCanonicalizer.SYSTEM)), apiManager);
//
//      queryGenerator.walk();
//      queryGenerator.getQueries().forEach(apiManager::addQuery);
//    } catch (Exception e) {
//      errors.withSchema(source.getName().getDisplay(), source.getSchemaDefinition()).handle(e);
//      return null;
//    }
//<<<<<<< HEAD
//    return Triple.of(null, null, apiManager);
//=======
//
//    //Build queries
//    SchemaBuilder schemaBuilder = new SchemaBuilder(source, apiManager);
//
//    RootGraphqlModel root = inferredSchema.accept(schemaBuilder, null);
//
//    return Pair.of(inferredSchema, root);

  @SneakyThrows
  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {
    inferSchemaAndQueries(script, schemaPath);
    PhysicalDAGPlan dag = injector.getInstance(DAGPlanner.class).plan();

    IndexSelector indexSelector = new IndexSelector(framework,
        IndexSelectorConfigByDialect.of("POSTGRES"));
    List<QueryIndexSummary> allIndexes = new ArrayList<>();
    for (PhysicalDAGPlan.ReadQuery query : dag.getReadQueries()) {
      List<QueryIndexSummary> queryIndexSummary = indexSelector.getIndexSelection(query);
      allIndexes.addAll(queryIndexSummary);
    }
    return indexSelector.optimizeIndexes(allIndexes);
  }

  protected void inferSchemaModelQueriesErr(String schema, SqrlFramework framework,
      ErrorCollector errors) {
    errors = errors.withSchema("schema", schema);
    try {
      inferSchemaModelQueries(schema, framework, errors);
      fail("Expected errors");
    } catch (Exception e) {
      errors.handle(e);
    }
  }
}