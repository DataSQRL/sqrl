/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelector;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.QueryIndexSummary;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.util.SqlNameUtil;
import com.datasqrl.plan.queries.APISourceImpl;
import com.datasqrl.util.TestScript;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;

public class AbstractSchemaInferenceModelTest extends AbstractLogicalSQRLIT {

  @SneakyThrows

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
  @SneakyThrows
  public void inferSchemaAndQueries(TestScript script,
      Path schemaPath) {
    plan(script.getScript());
    inferSchemaModelQueries(Files.readString(schemaPath), framework,
        errors);

  }
  @SneakyThrows
  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {
    inferSchemaAndQueries(script, schemaPath);
    PhysicalDAGPlan dag = injector.getInstance(DAGPlanner.class).plan();

    IndexSelector indexSelector = new IndexSelector(framework,
        IndexSelectorConfigByDialect.of(JdbcDialect.Postgres));
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