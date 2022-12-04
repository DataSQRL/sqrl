package com.datasqrl.graphql;

import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.Launcher;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import java.util.Optional;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class GraphQLServerCli implements Runnable {

  @Option(names = {"-l", "--listen"}, description = "Port to listen on", defaultValue = "8888")
  private int port = 8080;

  @Option(names = {"-e", "--env"}, description = "Use connection details from env variables")
  private boolean env = false;

  @Option(names = {"-c", "--connectionConfig"}, description = "Connection config")
  private Optional<String> connectionConfig = Optional.empty();

  @Option(names = {"-m",
      "--model"}, description = "The SQRL model definition", defaultValue = "model.conf")
  private String modelConfig = "model.conf";

  public static void main(String[] args) {
    new CommandLine(new GraphQLServerCli()).execute(args);
  }

  @Override
  public void run() {
//    Launcher.executeCommand("run", GraphQLServer.class.getName() /*, "--conf", "config.json"*/);
  }
}
