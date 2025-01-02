/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

import java.io.IOError;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ObjectUtils;

import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Stopwatch;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.netty.handler.codec.base64.Base64Decoder;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.micrometer.MicrometerMetricsOptions;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "compilation-server", description = "Compiles an SQRL script and produces all build artifacts")
public class CompilationServerCommand extends AbstractCompilerCommand {

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.COMPILE;
  }
  
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class CompileRequest {

    private String scriptFile;

    private String apiSpecFile;

    private List<String> packageFiles;

    private List<String> profiles;

    private Map<String, String> embeddedFiles;
  }
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public class PackageFile {
	  
	  private String name;
	  
	  private String content;
	  
  }

  @SneakyThrows
  public void run() {
	  PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(
		        PrometheusConfig.DEFAULT);
		    MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
		        .setMicrometerRegistry(prometheusMeterRegistry)
		        .setEnabled(true);
		    
		    VertxOptions options = new VertxOptions()
		            .setEventLoopPoolSize(1)       // Only 1 event loop
		            .setWorkerPoolSize(1)          // Only 1 worker thread
		            .setInternalBlockingPoolSize(1) // Also set internal blocking pool size to 1
		            .setMetricsOptions(metricsOptions);
		    
		    Vertx vertx = Vertx.vertx(options);

	        HttpServer server = vertx.createHttpServer();

	        Router router = Router.router(vertx);

	        // Accept POST bodies
	        router.route().handler(BodyHandler.create());

	        // Example: POST /compile
	        router.post("/compile")  
	        .handler(BodyHandler.create())
	        .blockingHandler(this::handleCompile, true);

	        // Start server on 7775 (sqrl in t9)
	        server.requestHandler(router).listen(7775, http -> {
	            if (http.succeeded()) {
	                System.out.println("HTTP server started on port 7775");
	            } else {
	                System.err.println("HTTP server failed to start: " + http.cause());
	            }
	        });
	        
	        Thread.currentThread().join();
  }
  
  @SneakyThrows
  void handleCompile(RoutingContext ctx) {
	  	Stopwatch sw = Stopwatch.createStarted();
	    CompileRequest request = ctx.body().asJsonObject().mapTo(CompileRequest.class);
	    
	    Path workDir = Files.createTempDirectory("sqrl");
	    root = new RootCommand(workDir);
	    
	    if(isEmpty(request.getEmbeddedFiles())) {
	    	ctx.fail(400);
	    	return;
	    }
	    
	    request.getEmbeddedFiles().forEach( (name, contents) -> {
	    	try {
				Path file = workDir.resolve(name);
				Files.createDirectories(file.getParent());
				Files.write(file, Base64.getDecoder().decode(contents) );
			} catch (IOException e) {
				throw new IOError(e);
			}
	    });
	    
	    targetDir = workDir.resolve("build");
	    
	    if(!isEmpty(request.getScriptFile())) {
	    Path script = workDir.resolve(request.getScriptFile());
	    
	    if(!isEmpty(request.getApiSpecFile())) {
	    	 Path apiSpec = workDir.resolve(request.getApiSpecFile());
	 	    
	 	    files = new Path[] {script, apiSpec};
	    } else {
	    	 files = new Path[] {script};
	    }
	    }
	    
	    if(!isEmpty(request.getPackageFiles())) {
	    	root.packageFiles = new ArrayList<>();
	    	for (String packageFile : request.getPackageFiles()) {
	 	 	  root.packageFiles.add(workDir.resolve(packageFile));
			}
	    }
	    
	    ErrorCollector collector = ErrorCollector.root();
	    try {
	      execute(collector);
	      root.statusHook.onSuccess();
	    } catch (CollectedException e) {
	      if (e.isInternalError()) e.printStackTrace();
	      e.printStackTrace();
	      root.statusHook.onFailure(e, collector);
	    } catch (Throwable e) { //unknown exception
	      collector.getCatcher().handle(e);
	      e.printStackTrace();
	      root.statusHook.onFailure(e, collector);
	    }

	    
	    if (collector.hasErrors()) {
	    	ctx.fail(400);
	    }
	    
	    ctx.json(ErrorPrinter.prettyPrint(collector));
	    sw.stop();
	    
	    log.info(String.format("Compilation finished after %ss", sw.elapsed(TimeUnit.SECONDS)));
  }

}
