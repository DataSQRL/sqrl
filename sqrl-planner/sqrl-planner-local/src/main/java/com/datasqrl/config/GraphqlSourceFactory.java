package com.datasqrl.config;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.ScriptConfiguration;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISourceImpl;
import com.google.inject.Inject;
import java.util.Optional;

public class GraphqlSourceFactory {
  Optional<APISource> apiSchemaOpt;

  @Inject
  public GraphqlSourceFactory(ScriptFiles scriptFiles, NameCanonicalizer nameCanonicalizer,
      ResourceResolver resourceResolver) {
    apiSchemaOpt = scriptFiles.get(ScriptConfiguration.GRAPHQL_KEY)
        .map(file -> APISourceImpl.of(file, nameCanonicalizer, resourceResolver));
  }

  public Optional<APISource> get() {
    return apiSchemaOpt;
  }
}
