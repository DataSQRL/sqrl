package com.datasqrl.cmd;

import java.io.IOException;

import com.datasqrl.auth.AuthProvider;
import com.datasqrl.error.ErrorCollector;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@CommandLine.Command(name = "login", description = "Logs into the repository")
@Slf4j
public class LoginCommand extends AbstractCommand {

  @Override
  protected void execute(ErrorCollector errors) throws IOException {
    var authProvider = new AuthProvider();
    authProvider.loginToRepository();
    log.info("Login successful.");
  }

}
