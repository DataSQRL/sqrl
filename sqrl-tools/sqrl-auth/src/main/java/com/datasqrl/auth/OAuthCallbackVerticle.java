package com.datasqrl.auth;

import static com.datasqrl.auth.AuthUtils.CALLBACK_ENDPOINT;
import static com.datasqrl.auth.AuthUtils.CALLBACK_SERVER_PORT;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.function.Consumer;

public class OAuthCallbackVerticle extends AbstractVerticle {

    private final Consumer<String> onOAuthCallback;

    public OAuthCallbackVerticle(Consumer<String> onOAuthCallback) {
        this.onOAuthCallback = onOAuthCallback;
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        Router router = Router.router(vertx);

        router.route(CALLBACK_ENDPOINT).handler(this::handleAuthCallback);

        vertx.createHttpServer().requestHandler(router).listen(CALLBACK_SERVER_PORT, http -> {
            if (http.succeeded()) {
                startPromise.complete();
            } else {
                startPromise.fail(http.cause());
            }
        });
    }

    private void handleAuthCallback(RoutingContext routingContext) {
        String code = routingContext.request().getParam("code");

        routingContext.response()
                .putHeader("content-type", "text/html")
                .end("Authentication successful. You can close this window.");

        onOAuthCallback.accept(code);
    }

}
