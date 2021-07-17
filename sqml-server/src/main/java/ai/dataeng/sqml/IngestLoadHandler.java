//package ai.dataeng.sqml;
//
//import com.google.common.base.Preconditions;
//import io.vertx.core.Handler;
//import io.vertx.ext.web.FileUpload;
//import io.vertx.ext.web.RoutingContext;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.Optional;
//import java.util.Set;
//
//public class IngestLoadHandler implements Handler<RoutingContext> {
//  private ScriptManager sqmlScriptManager;
//  public IngestLoadHandler(ScriptManager sqmlScriptManager) {
//    this.sqmlScriptManager = sqmlScriptManager;
//  }
//
//  @Override
//  public void handle(RoutingContext ctx) {
//    String name = getScriptName(ctx.pathParams().get("*"));
//
//    FileUpload ingest = getFileUpload("ingest", ctx.fileUploads());
//
//    Optional<String> ingestFile = getFileAsString(ingest);
//
//    if (ingestFile.isEmpty()) {
//      ctx.response()
//          .setStatusCode(200)
//          .putHeader("Content-Type", "application/json")
//          .end("{\"success\": false}");
//      return;
//    }
//
//    sqmlScriptManager.loadIngest(name, ingestFile.get());
//
//    ctx.response()
//        .setStatusCode(200)
//        .putHeader("Content-Type", "application/json")
//        .end("{\"success\": true}");
//  }
//
//  private Optional<String> getFileAsString(FileUpload file) {
//    if (file == null) {
//      return Optional.empty();
//    }
//    try {
//      return Optional.of(new String(Files.readAllBytes(Path.of(file.uploadedFileName()))));
//    } catch (IOException e) {
//      return Optional.empty();
//    }
//  }
//
//  private FileUpload getFileUpload(String name, Set<FileUpload> fileUploads) {
//    for (FileUpload fileUpload : fileUploads) {
//      if (!fileUpload.name().equals(name)) {
//        return fileUpload;
//      }
//    }
//    return null;
//  }
//
//  public String getScriptName(String path) {
//    Preconditions.checkState(!path.contains("/"), "Script name cannot contain a subdirectory");
//    return path;
//  }
//}
