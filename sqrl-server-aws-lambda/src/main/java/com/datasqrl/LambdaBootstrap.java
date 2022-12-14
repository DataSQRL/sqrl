/*
 * Copyright 2019 Paulo Lopes.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *  The Eclipse Public License is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 *  The Apache License v2.0 is available at
 *  http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */
package com.datasqrl;

import static java.lang.System.getenv;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.security.Security;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Main entrypoint for the application.
 */
public class LambdaBootstrap {

  private static final String LAMBDA_VERSION_DATE = "2018-06-01";

  private static final String LAMBDA_RUNTIME_TEMPLATE = "/{0}/runtime/invocation/next";
  private static final String LAMBDA_INVOCATION_TEMPLATE = "/{0}/runtime/invocation/{1}/response";
  private static final String LAMBDA_INIT_ERROR_TEMPLATE = "/{0}/runtime/init/error";
  private static final String LAMBDA_ERROR_TEMPLATE = "/{0}/runtime/invocation/{1}/error";

  private static final Map<String, Lambda> HANDLERS = new HashMap<>();
  static {
//    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

    System.setProperty("vertx.disableDnsResolver", "true");
    System.setProperty("vertx.cacheDirBase", "/tmp/vertx-cache");
    System.setProperty("java.net.preferIPv4Stack", "true");

    // load all handlers available, if this becomes a performance
    ServiceLoader<Lambda> serviceLoader = ServiceLoader.load(Lambda.class);
    for (Lambda fn : serviceLoader) {
      HANDLERS.put(fn.getClass().getName(), fn);
    }
  }

  public static void main(String[] args) {
    try {
      new LambdaBootstrap(Vertx.vertx());
    } catch (RuntimeException e) {
      e.printStackTrace();
//      System.exit(1);
    }
  }

  private final WebClient client;

  private final Lambda fn;

  private final String host;
  private final int port;

  private LambdaBootstrap(Vertx vertx) {
    // create an WebClient
    this.client = WebClient.create(vertx);

    String runtimeApi = getenv("AWS_LAMBDA_RUNTIME_API");

    int sep = runtimeApi.indexOf(':');
    if (sep != -1) {
      host = runtimeApi.substring(0, sep);
      port = Integer.parseInt(runtimeApi.substring(sep + 1));
    } else {
      host = runtimeApi;
      port = 80;
    }

    // Get the handler class and method name from the Lambda Configuration in the format of <fqcn>
    this.fn = HANDLERS.get(getenv("_HANDLER"));
    final String runtimeUrl = MessageFormat.format(LAMBDA_RUNTIME_TEMPLATE, LAMBDA_VERSION_DATE);

    if (fn == null) {
      // Not much else to do handler can't be found.
      final String uri = MessageFormat.format(LAMBDA_INIT_ERROR_TEMPLATE, LAMBDA_VERSION_DATE);
      fail(uri, "Could not find handler method", "InitError");
    } else {
      process(vertx, runtimeUrl);
    }
  }

  private void process(Vertx vertx, String runtimeUrl) {
    client.get(port, host, runtimeUrl).send(getAbs -> {
      if (getAbs.succeeded()) {
        HttpResponse<Buffer> response = getAbs.result();

        if (response.statusCode() != 200) {
          System.exit(0);
        }

        String requestId = response.getHeader("Lambda-Runtime-Aws-Request-Id");

        try {
          // Invoke Handler Method
          fn.call(vertx, response.headers(), response.body())
            .onComplete(ar -> {
              if (ar.succeeded()) {
                // Post the results of Handler Invocation
                String invocationUrl = MessageFormat.format(LAMBDA_INVOCATION_TEMPLATE, LAMBDA_VERSION_DATE, requestId);
                success(invocationUrl, ar.result(), ack -> {
                  if (ack.failed()) {
                    ack.cause().printStackTrace();
                    // terminate the process
                    System.exit(1);
                  } else {
                    // process the next call
                    // run on context to avoid large stacks
                    vertx.runOnContext(v -> process(vertx, runtimeUrl));
                  }
                });
              } else {
                String initErrorUrl = MessageFormat.format(LAMBDA_ERROR_TEMPLATE, LAMBDA_VERSION_DATE, requestId);
                fail(initErrorUrl, "Invocation Error", "RuntimeError");
              }
            });

        } catch (Exception e) {
          String initErrorUrl = MessageFormat.format(LAMBDA_ERROR_TEMPLATE, LAMBDA_VERSION_DATE, requestId);
          fail(initErrorUrl, "Invocation Error", "RuntimeError");
        }
      } else {
        getAbs.cause().printStackTrace();
        System.exit(1);
      }
    });
  }

  private void success(String requestURI, Buffer result, Handler<AsyncResult<Void>> handler) {
    client.post(port, host, requestURI)
      .sendBuffer(result, ar -> {
        if (ar.succeeded()) {
          // we don't really care about the response
          handler.handle(Future.succeededFuture());
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
  }

  private void fail(String requestURI, String errMsg, String errType) {
    final JsonObject error = new JsonObject()
      .put("errorMessage", errMsg)
      .put("errorType", errType);

    client.post(port, host, requestURI)
      .sendJson(error, ar -> {
        if (ar.failed()) {
          ar.cause().printStackTrace();
        }
        // terminate the process
        System.exit(1);
      });
  }
}
