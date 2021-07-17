//package ai.dataeng.sqml;
//
//import com.google.common.base.Preconditions;
//import io.vertx.core.Handler;
//import io.vertx.ext.web.RoutingContext;
//
//public class SqmlScriptUnloadHandler implements Handler<RoutingContext> {
//  private ScriptManager sqmlScriptManager;
//  public SqmlScriptUnloadHandler(ScriptManager sqmlScriptManager) {
//    this.sqmlScriptManager = sqmlScriptManager;
//  }
//
//  @Override
//  public void handle(RoutingContext ctx) {
//    String name = getScriptName(ctx.pathParams().get("*"));
//    boolean unloaded = sqmlScriptManager.unloadScript(name);
//
//    ctx.response()
//        .setStatusCode(200)
//        .putHeader("Content-Type", "application/json")
//        .end("{\"success\": " + unloaded + "}");
//  }
//  public String getScriptName(String path) {
//    Preconditions.checkState(!path.contains("/"), "Script name cannot contain a subdirectory");
//    return path;
//  }
//}
