package com.datasqrl.graphql.auth;

import com.datasqrl.graphql.server.MetadataReader;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.ext.auth.impl.jose.JWT;
import io.vertx.ext.web.RoutingContext;

public class JwtMetadataReader implements MetadataReader {

  @Override
  public Object read(DataFetchingEnvironment env, String name) {
    RoutingContext rc =
        env.getGraphQlContext()
            .get(RoutingContext.class); // Vert.x ≥ 4.2 / 5.x :contentReference[oaicite:0]{index=0}

    var headers = rc.request().headers();
    var auth = headers.get("Authorization");

    if (auth == null || !auth.startsWith("Bearer ")) {
      throw new IllegalStateException("Missing bearer token");
    }

    // no need to validate, if we reached this, Authorization already validated the tokens
    var claims =
        JWT.parse(
            auth.substring(
                "Bearer ".length())); // static helper :contentReference[oaicite:1]{index=1}
    return claims.getJsonObject("payload").getValue(name);
  }
}
