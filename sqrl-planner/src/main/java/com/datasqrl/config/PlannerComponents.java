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
package com.datasqrl.config;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.loaders.ClasspathFunctionLoader;
import com.datasqrl.loaders.ModuleLoaders;
import com.datasqrl.plan.MainScriptImpl;
import com.datasqrl.planner.SqlScriptPlanner;
import com.datasqrl.planner.dag.DAGPlanner;
import com.datasqrl.planner.parser.SqrlStatementParser;
import com.datasqrl.server.GraphqlSchemaFactory;
import com.datasqrl.server.GraphqlSchemaHandler;
import com.datasqrl.server.ScriptFiles;
import com.datasqrl.server.ServerModelManager;
import com.datasqrl.server.converter.GraphQLSchemaConverter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
  TypeFactory.class,
  ConnectorFactoryFactoryImpl.class,
  ExecutionEnginesHolder.class,
  GraphqlSourceLoader.class,
  QueryEngineConfigConverterImpl.class,
  SqrlCompilerConfiguration.class,
  SqrlConfigPipeline.class,
  ClasspathFunctionLoader.class,
  ModuleLoaders.class,
  MainScriptImpl.class,
  DAGPlanner.class,
  SqrlStatementParser.class,
  SqlScriptPlanner.class,
  GraphQLSchemaConverter.class,
  GraphqlSchemaFactory.class,
  GraphqlSchemaHandler.class,
  ScriptFiles.class,
  ServerModelManager.class
})
public class PlannerComponents {}
