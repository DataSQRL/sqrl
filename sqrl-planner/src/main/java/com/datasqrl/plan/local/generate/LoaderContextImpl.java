///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl.plan.local.generate;
//
//import com.datasqrl.function.SqrlFunction;
//import com.datasqrl.loaders.LoaderContext;
//import com.datasqrl.error.ErrorCollector;
//import com.datasqrl.io.tables.TableSource;
//import com.datasqrl.name.Name;
//import com.datasqrl.name.NamePath;
//import com.datasqrl.util.ResourceResolver;
//import lombok.Getter;
//import lombok.Value;
//
//import java.nio.file.Path;
//import java.util.Optional;
//import org.apache.flink.table.functions.UserDefinedFunction;
//
//@Value
//class LoaderContextImpl implements LoaderContext {
//
//  Env env;
//  Namespace namespace;
//
//  ResourceResolver resourceResolver;
//
//  @Getter
//  NamePath namePath;
//
//  @Override
//  public Path getPackagePath() {
//    return env.getPackagePath();
//  }
//
//
//  public Namespace getNamespace() {
//    return namespace;
//  }
//
//  @Override
//  public ErrorCollector getErrorCollector() {
//    return env.errors;
//  }
//
//  @Override
//  public Name registerTable(TableSource tbl, Optional<Name> alias) {
//    return env.registerTable(tbl, alias);
//  }
//
//  @Override
//  public boolean registerFunction(Name name, UserDefinedFunction udf) {
//    env.getTableEnv()
//        .createTemporarySystemFunction(name.getCanonical(), udf);
//
//    return true;
//  }
//
//  @Override
//  public ResourceResolver getResourceResolver() {
//    return resourceResolver;
//  }
//}
