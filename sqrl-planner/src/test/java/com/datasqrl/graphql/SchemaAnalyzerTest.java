///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl.graphql;
//
//import com.datasqrl.AbstractLogicalSQRLIT;
//import com.datasqrl.IntegrationTestSettings;
//import com.datasqrl.graphql.inference.SchemaInference;
//import com.datasqrl.plan.local.generate.Resolve;
//import com.datasqrl.plan.local.generate.Resolve.Env;
//import graphql.schema.idl.SchemaParser;
//import graphql.schema.idl.TypeDefinitionRegistry;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import lombok.SneakyThrows;
//import org.apache.calcite.sql.ScriptNode;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//
//@Disabled
//class SchemaAnalyzerTest extends AbstractLogicalSQRLIT {
//
//  Path DIR_BASE = Path.of("../sqrl-examples/starwars/starwars");
//
//  @BeforeEach
//  public void before() {
//    initialize(IntegrationTestSettings.getInMemory(), DIR_BASE);
//  }
//
//  @SneakyThrows
//  @Test
//  @Disabled
//  public void test() {
//    //TODO: This is broken (see #plan()). Fix or remove test
//    Resolve resolve = new Resolve(DIR_BASE);
//    ScriptNode node = parser.parse("starwars.sqrl",error);
//    Env env2 = resolve.planDag(session, node, error);
//
//    String gql = Files.readString(Path.of("../sqrl-examples/starwars")
//        .resolve("starwars.graphql"));
//
//    TypeDefinitionRegistry typeDefinitionRegistry =
//        (new SchemaParser()).parse(gql);
//
//    SchemaInference inference = new SchemaInference(gql, env2.getRelSchema(),
//        env2.getSession().createRelBuilder()
//    );
//    inference.accept();
//  }
//}