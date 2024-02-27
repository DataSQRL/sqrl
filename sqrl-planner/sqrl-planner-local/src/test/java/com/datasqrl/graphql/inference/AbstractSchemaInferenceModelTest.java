/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import static com.datasqrl.plan.SqrlOptimizeDag.extractFlinkFunctions;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.QueryIndexSummary;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelector;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.local.analyze.MockAPIConnectorManager;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.util.SqlNameUtil;
import com.datasqrl.util.TestScript;
import graphql.schema.idl.SchemaParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public class AbstractSchemaInferenceModelTest extends AbstractLogicalSQRLIT {

  public AbstractSchemaInferenceModelTest() {
    this.errors = ErrorCollector.root();
  }

  @SneakyThrows
  public Pair<Object, APIConnectorManager> inferSchemaAndQueries(TestScript script,
      Path schemaPath) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory(), Optional.empty());
    String schemaStr = Files.readString(schemaPath);
    plan(script.getScript());
    Triple<Object, RootGraphqlModel, APIConnectorManager> result = inferSchemaModelQueries(
        schemaStr, framework, pipeline, errors);
    return Pair.of(result.getLeft(), result.getRight());
  }

  public static Triple<Object, RootGraphqlModel, APIConnectorManager> inferSchemaModelQueries(
      String schemaStr, SqrlFramework framework, ExecutionPipeline pipeline, ErrorCollector errors) {
    APISource source = APISource.of(schemaStr);
    //Inference
//    SqrlSchemaForInference sqrlSchemaForInference = new SqrlSchemaForInference(framework.getSchema());

    MockAPIConnectorManager apiManager = new MockAPIConnectorManager(framework, pipeline);

//    SchemaInference inference = new SchemaInference(framework, "<schema>", null,source,
//        sqrlSchemaForInference,
//        framework.getQueryPlanner().getRelBuilder(), apiManager);
//
//    InferredSchema inferredSchema;
//    try {
//      inferredSchema = inference.accept();
//    } catch (Exception e) {
//      errors.withSchema("<schema>", schemaStr).handle(e);
//      return null;
//    }

    GraphqlQueryGenerator queryGenerator = new GraphqlQueryGenerator(framework.getCatalogReader().nameMatcher(),
        framework.getSchema(),  (new SchemaParser()).parse(source.getSchemaDefinition()), source,
        new GraphqlQueryBuilder(framework, apiManager, new SqlNameUtil(NameCanonicalizer.SYSTEM)), apiManager);

    queryGenerator.walk();
    queryGenerator.getQueries().forEach(apiManager::addQuery);
    System.out.println();

    //Build queries
//    SchemaBuilder3 schemaBuilder = new SchemaBuilder3(framework.getCatalogReader().nameMatcher(),
//        framework.getSchema(), (new SchemaParser()).parse(source.getSchemaDefinition()), source) {
//
//      @Override
//      protected Object visitScalar(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
//          RelDataType relDataType, RelDataTypeField relDataTypeField) {
//        return null;
//      }
//
//      @Override
//      protected void visitQuery(ObjectTypeDefinition parentType, ObjectTypeDefinition type,
//          FieldDefinition field, NamePath path, Optional<RelDataType> rel,
//          List<SqrlTableMacro> functions) {
//        System.out.println();
//      }
//    };
//    schemaBuilder.walk();

//    RootGraphqlModel root = schemaBuilder.build(framework.getSchema());//inferredSchema.accept(schemaBuilder, null);

    return Triple.of(null, null, apiManager);
  }

  public Map<IndexDefinition, Double> selectIndexes(TestScript script, Path schemaPath) {
    APIConnectorManager apiManager = inferSchemaAndQueries(script, schemaPath).getValue();
    // plan dag
    PhysicalDAGPlan dag = DAGPlanner.plan(framework,
        apiManager, framework.getSchema().getExports(),
        framework.getSchema().getJars(), extractFlinkFunctions(framework.getSqrlOperatorTable()), null, pipeline,
        errors, debugger);

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