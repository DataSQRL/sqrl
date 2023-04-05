package com.datasqrl.cmd;

public class GenerateCommand {

  public static void main(String[] args) {
    new RootGenerateCommand().getCmd().execute(args);
  }

}
