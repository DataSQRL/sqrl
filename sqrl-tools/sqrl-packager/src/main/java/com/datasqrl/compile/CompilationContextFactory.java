//package com.datasqrl.compile;
//
//import static com.datasqrl.config.PipelineFactory.ENGINES_PROPERTY;
//
//import com.datasqrl.calcite.SqrlFramework;
//import com.datasqrl.calcite.SqrlFrameworkFactory;
//import com.datasqrl.config.PipelineFactory;
//import com.datasqrl.config.SqrlConfig;
//import com.datasqrl.config.SqrlConfigCommons;
//import com.datasqrl.engine.pipeline.ExecutionPipeline;
//import com.datasqrl.error.ErrorCollector;
//import com.datasqrl.graphql.APIConnectorManager;
//import com.datasqrl.graphql.APIConnectorManagerImpl;
//import com.datasqrl.hooks.CompilerContext;
//import com.datasqrl.loaders.ModuleLoader;
//import com.datasqrl.loaders.ModuleLoaderImpl;
//import com.datasqrl.loaders.ObjectLoaderImpl;
//import com.datasqrl.module.resolver.FileResourceResolver;
//import com.datasqrl.module.resolver.ResourceResolver;
//import com.datasqrl.packager.Packager;
//import com.datasqrl.plan.table.CalciteTableFactory;
//import java.nio.file.Path;
//import java.util.Optional;
//
//public class CompilationContextFactory {
//
//  public static CompilerContext createContext(ErrorCollector errors, Path buildDir, Path targetDir,
//      boolean debug) {
//    SqrlConfig config = SqrlConfigCommons.fromFiles(errors, buildDir.resolve(Packager.PACKAGE_JSON));
//    ExecutionPipeline pipeline = new PipelineFactory(config.getSubConfig(ENGINES_PROPERTY))
//        .createPipeline();
//    SqrlFramework framework = new SqrlFrameworkFactory()
//        .createFramework();
//
//    ResourceResolver resourceResolver = new FileResourceResolver(buildDir);
//    ModuleLoader baseModuleLoader = new ModuleLoaderImpl(new ObjectLoaderImpl(resourceResolver, errors,
//        new CalciteTableFactory(framework)));
//
//    APIConnectorManager apiManager = new APIConnectorManagerImpl(
//        new CalciteTableFactory(framework),
//        pipeline, errors, baseModuleLoader, framework.getTypeFactory());
//
//    ModuleLoader moduleLoader = ModuleLoaderComposite.builder()
//        .moduleLoader(baseModuleLoader)
//        .moduleLoader(apiManager.getAsModuleLoader())
//        .build();
//
//    CompilerContext context = new CompilerContext(errors, pipeline, framework,
//        moduleLoader, apiManager, resourceResolver, config, targetDir, debug, Optional.empty());
//
//    return context;
//  }
//}
