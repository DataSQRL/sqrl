package com.datasqrl.config;

import java.util.Optional;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.graphql.APISource;
import com.datasqrl.graphql.APISourceImpl;
import com.google.inject.Inject;

public class GraphqlSourceFactory {
  Optional<APISource> apiSchemaOpt;

  @Inject
  public GraphqlSourceFactory(ScriptFiles scriptFiles, NameCanonicalizer nameCanonicalizer,
      ResourceResolver resourceResolver) {
    apiSchemaOpt = scriptFiles.getConfig().getGraphql()
        .map(file -> APISourceImpl.of(file, nameCanonicalizer, resourceResolver));
  }

  public Optional<APISource> get() {
    return apiSchemaOpt;
  }
}
