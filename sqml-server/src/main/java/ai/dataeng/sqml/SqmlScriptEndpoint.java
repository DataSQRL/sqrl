//package ai.dataeng.sqml;
//
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.Promise;
//import io.vertx.core.http.HttpServer;
//import io.vertx.core.http.HttpServerOptions;
//import io.vertx.ext.web.Router;
//import io.vertx.ext.web.handler.BodyHandler;
//
//public class SqmlScriptEndpoint extends AbstractVerticle {
//
//  private final ScriptManager sqmlScriptManager;
//  HttpServer server;
//
//  public SqmlScriptEndpoint(ScriptManager sqmlScriptManager) {
//
//    this.sqmlScriptManager = sqmlScriptManager;
//  }
//
//  @Override
//  public void start(Promise<Void> promise) throws Exception {
//    server = vertx.createHttpServer(createOptions());
//
//    server.requestHandler(createRouter());
//    server.listen(res -> {
//      if (res.failed()) {
//        System.out.println("Started SQML script endpoint...Failed");
//        promise.fail(res.cause());
//      } else {
//        System.out.println("Started SQML script endpoint");
//        promise.complete();
//      }
//    });
//  }
//
//  private Router createRouter() {
//    Router router = Router.router(vertx);
//    //Todo: Find out why this is required
//    router.post("/load/*").handler(BodyHandler.create().setMergeFormAttributes(false));
//    router.post("/load/*").handler(new SqmlScriptLoadHandler(sqmlScriptManager));
//    router.post("/loadIngest/*").handler(BodyHandler.create().setMergeFormAttributes(false));
//    router.post("/loadIngest/*").handler(new IngestLoadHandler(sqmlScriptManager));
//    router.post("/unload/*").handler(new SqmlScriptUnloadHandler(sqmlScriptManager));
//    return router;
//  }
//
//  private HttpServerOptions createOptions() {
//    HttpServerOptions serverOptions = new HttpServerOptions()
//        .setPort(8080)
//        .setHost("localhost");
////    serverOptions.setSsl(true)
////        .setKeyCertOptions(new PemKeyCertOptions().setCertPath("tls/server-cert.pem").setKeyPath("tls/server-key.pem"))
////        .setUseAlpn(true);
//    return serverOptions;
//  }
//}
