/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.cmd.RootCommand;
import picocli.CommandLine;


public class DatasqrlCMD {

  public static void main(String[] args) {
    new CommandLine(new RootCommand()).execute(args);
  }

}