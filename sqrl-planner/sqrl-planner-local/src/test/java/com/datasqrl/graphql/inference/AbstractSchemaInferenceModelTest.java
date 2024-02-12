/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.calcite.SqrlFramework;
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
import org.apache.commons.lang3.tuple.Triple;

public class AbstractSchemaInferenceModelTest extends AbstractLogicalSQRLIT {

  @SneakyThrows
  public Pair<Object, APIConnectorManager> inferSchemaAndQueries(TestScript script,
      Path schemaPath) {
    String schemaStr = Files.readString(schemaPath);
    plan(script.getScript());
    Triple<Object, RootGraphqlModel, APIConnectorManager> result = inferSchemaModelQueries(
        schemaStr, framework, errors);
    return Pair.of(result.getLeft(), result.getRight());
  }

  public Triple<Object, RootGraphqlModel, APIConnectorManager> inferSchemaModelQueries(
      String schemaStr, SqrlFramework framework, ErrorCollector errors) {
    APISource source = APISourceImpl.of(schemaStr);
    //Inference
//    GraphQLMutationExtraction preAnalysis = new GraphQLMutationExtraction(
//        framework.getTypeFactory(),
//        NameCanonicalizer.SYSTEM);

    APIConnectorManager apiManager = this.injector.getInstance(APIConnectorManager.class);
    try {
      //todo readd once moved
//      preAnalysis.analyze(source);

      GraphqlSchemaValidator schemaValidator = new GraphqlSchemaValidator(
          framework.getCatalogReader().nameMatcher(),
          framework.getSchema(), source, (new SchemaParser()).parse(source.getSchemaDefinition()),
          apiManager);
      schemaValidator.validate(source, errors);

      GraphqlQueryGenerator queryGenerator = new GraphqlQueryGenerator(framework.getCatalogReader().nameMatcher(),
          framework.getSchema(),  (new SchemaParser()).parse(source.getSchemaDefinition()), source,
          new GraphqlQueryBuilder(framework, apiManager, new SqlNameUtil(NameCanonicalizer.SYSTEM)), apiManager);

      queryGenerator.walk();
      queryGenerator.getQueries().forEach(apiManager::addQuery);
    } catch (Exception e) {
      errors.withSchema(source.getName().getDisplay(), source.getSchemaDefinition()).handle(e);
      return null;
    }
    return Triple.of(null, null, apiManager);
  }

  @SneakyThrows
  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {

    APIConnectorManager apiManager = inferSchemaAndQueries(script, schemaPath).getValue();
    // plan dag
    PhysicalDAGPlan dag = new DAGPlanner(
        injector.getInstance(SqrlFramework.class),
        apiManager,
        injector.getInstance(ExecutionPipeline.class),
        injector.getInstance(ErrorCollector.class),
        injector.getInstance(DAGAssembler.class),
        injector.getInstance(DAGBuilder.class),
        injector.getInstance(DAGPreparation.class)).plan();

    IndexSelector indexSelector = new IndexSelector(framework,
        IndexSelectorConfigByDialect.of("POSTGRES"));
    List<QueryIndexSummary> allIndexes = new ArrayList<>();
    for (PhysicalDAGPlan.ReadQuery query : dag.getReadQueries()) {
      List<QueryIndexSummary> queryIndexSummary = indexSelector.getIndexSelection(query);
      allIndexes.addAll(queryIndexSummary);
    }
    return indexSelector.optimizeIndexes(allIndexes);
  }
}