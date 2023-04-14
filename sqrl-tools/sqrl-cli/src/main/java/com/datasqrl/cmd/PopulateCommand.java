/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "populate", description = "Compiles a SQRL script and runs the stream part of the data pipeline to populate the database")
public class PopulateCommand extends AbstractCompilerCommand {

  protected PopulateCommand() {
    super(true, false);
  }

}
