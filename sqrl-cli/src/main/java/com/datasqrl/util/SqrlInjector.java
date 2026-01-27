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
package com.datasqrl.util;

import com.datasqrl.MainScriptImpl;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.compile.CompilationProcess;
import com.datasqrl.compile.DagWriter;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.ConnectorFactoryFactoryImpl;
import com.datasqrl.config.ExecutionEnginesHolder;
import com.datasqrl.config.GraphqlSourceLoader;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.QueryEngineConfigConverter;
import com.datasqrl.config.QueryEngineConfigConverterImpl;
import com.datasqrl.config.RootPath;
import com.datasqrl.config.SqrlCompilerConfiguration;
import com.datasqrl.config.SqrlConfigPipeline;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.config.TargetPath;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.GenerateServerModel;
import com.datasqrl.graphql.GraphqlSchemaFactory;
import com.datasqrl.graphql.InferGraphqlSchema;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.graphql.converter.GraphQLSchemaConverter;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.preprocess.CopyStaticDataPreprocessor;
import com.datasqrl.packager.preprocess.JBangPreprocessor;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.packager.preprocess.Preprocessor;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.planner.SqlScriptPlanner;
import com.datasqrl.planner.dag.DAGPlanner;
import com.datasqrl.planner.parser.SqrlStatementParser;
import java.nio.file.Path;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SqrlInjector {

  @Bean
  public RelDataTypeFactory relDataTypeFactory() {
    return new TypeFactory();
  }

  @Bean
  public MainScript mainScript(PackageJson packageJson, ResourceResolver resourceResolver) {
    return new MainScriptImpl(packageJson, resourceResolver);
  }

  @Bean
  public ExecutionPipeline executionPipeline(ApplicationContext applicationContext) {
    return new SqrlConfigPipeline(applicationContext);
  }

  @Bean
  public ModuleLoader moduleLoader(
      ResourceResolver resourceResolver, BuildPath buildPath, ErrorCollector errors) {
    return new ModuleLoaderImpl(resourceResolver, buildPath, errors);
  }

  @Bean
  public CompilerConfig compilerConfig(PackageJson packageJson) {
    return new SqrlCompilerConfiguration(packageJson);
  }

  @Bean
  public ConnectorFactoryFactory connectorFactoryFactory(PackageJson packageJson) {
    return new ConnectorFactoryFactoryImpl(packageJson);
  }

  @Bean
  public QueryEngineConfigConverter queryEngineConfigConverter(
      ExecutionEnginesHolder enginesHolder, PackageJson packageJson) {
    return new QueryEngineConfigConverterImpl(enginesHolder, packageJson);
  }

  @Bean
  public Preprocessor copyStaticDataPreprocessor() {
    return new CopyStaticDataPreprocessor();
  }

  @Bean
  public Preprocessor jBangPreprocessor(JBangRunner jBangRunner) {
    return new JBangPreprocessor(jBangRunner);
  }

  @Bean
  public Preprocessor jarPreprocessor() {
    return new JarPreprocessor();
  }

  @Bean
  @Qualifier("buildDir")
  public Path buildDir(@Qualifier("rootDir") Path rootDir) {
    return rootDir.resolve(SqrlConstants.BUILD_DIR_NAME);
  }

  @Bean
  public BuildPath buildPath(
      @Qualifier("buildDir") Path buildDir, @Qualifier("targetDir") Path targetDir) {
    return new BuildPath(buildDir, targetDir);
  }

  @Bean
  public ResourceResolver resourceResolver(@Qualifier("buildDir") Path buildDir) {
    return new FileResourceResolver(buildDir);
  }

  @Bean
  public NameCanonicalizer nameCanonicalizer() {
    return NameCanonicalizer.SYSTEM;
  }

  @Bean
  public JBangRunner jBangRunner(@Qualifier("internalTestExec") Boolean internalTestExec) {
    return internalTestExec ? JBangRunner.disabled() : JBangRunner.create();
  }

  @Bean
  public ExecutionEnginesHolder executionEnginesHolder(
      ErrorCollector errors,
      ApplicationContext applicationContext,
      PackageJson sqrlConfig,
      ExecutionGoal goal) {
    return new ExecutionEnginesHolder(
        errors, applicationContext, sqrlConfig, goal == ExecutionGoal.TEST);
  }

  @Bean
  public RootPath rootPath(@Qualifier("rootDir") Path rootDir) {
    return new RootPath(rootDir);
  }

  @Bean
  public TargetPath targetPath(@Qualifier("targetDir") Path targetDir) {
    return new TargetPath(targetDir);
  }

  @Bean
  public FilePreprocessingPipeline filePreprocessingPipeline(
      BuildPath buildPath, Set<Preprocessor> preprocessors) {
    return new FilePreprocessingPipeline(buildPath, preprocessors);
  }

  @Bean
  public Packager packager(
      RootPath rootPath,
      PackageJson packageJson,
      BuildPath buildPath,
      FilePreprocessingPipeline preprocPipeline,
      MainScript mainScript) {
    return new Packager(rootPath, packageJson, buildPath, preprocPipeline, mainScript);
  }

  @Bean
  public SqrlStatementParser sqrlStatementParser() {
    return new SqrlStatementParser();
  }

  @Bean
  public SqlScriptPlanner sqlScriptPlanner(
      ErrorCollector errorCollector,
      ModuleLoader moduleLoader,
      SqrlStatementParser sqrlParser,
      PackageJson packageJson,
      ExecutionPipeline pipeline,
      ExecutionGoal executionGoal) {
    return new SqlScriptPlanner(
        errorCollector, moduleLoader, sqrlParser, packageJson, pipeline, executionGoal);
  }

  @Bean
  public DAGPlanner dagPlanner(
      ExecutionPipeline pipeline, ErrorCollector errors, PackageJson packageJson) {
    return new DAGPlanner(pipeline, errors, packageJson);
  }

  @Bean
  public GraphQLSchemaConverter graphQLSchemaConverter() {
    return new GraphQLSchemaConverter();
  }

  @Bean
  public GenerateServerModel generateServerModel(
      PackageJson configuration, ErrorCollector errorCollector, GraphQLSchemaConverter converter) {
    return new GenerateServerModel(configuration, errorCollector, converter);
  }

  @Bean
  public GraphqlSchemaFactory graphqlSchemaFactory(CompilerConfig config) {
    return new GraphqlSchemaFactory(config);
  }

  @Bean
  public InferGraphqlSchema inferGraphqlSchema(
      ErrorCollector errorCollector, GraphqlSchemaFactory graphqlSchemaFactory) {
    return new InferGraphqlSchema(errorCollector, graphqlSchemaFactory);
  }

  @Bean
  public ScriptFiles scriptFiles(PackageJson rootConfig) {
    return new ScriptFiles(rootConfig);
  }

  @Bean
  public GraphqlSourceLoader graphqlSourceLoader(
      ScriptFiles scriptFiles, ResourceResolver resolver) {
    return new GraphqlSourceLoader(scriptFiles, resolver);
  }

  @Bean
  public DagWriter dagWriter(BuildPath buildDir, CompilerConfig compilerConfig) {
    return new DagWriter(buildDir, compilerConfig);
  }

  @Bean
  public CompilationProcess compilationProcess(
      SqlScriptPlanner planner,
      DAGPlanner dagPlanner,
      BuildPath buildPath,
      MainScript mainScript,
      PackageJson config,
      GenerateServerModel generateServerModel,
      InferGraphqlSchema inferGraphqlSchema,
      DagWriter writeDeploymentArtifactsHook,
      GraphqlSourceLoader graphqlSourceLoader,
      ExecutionGoal executionGoal,
      ErrorCollector errors) {
    return new CompilationProcess(
        planner,
        dagPlanner,
        buildPath,
        mainScript,
        config,
        generateServerModel,
        inferGraphqlSchema,
        writeDeploymentArtifactsHook,
        graphqlSourceLoader,
        executionGoal,
        errors);
  }
}
