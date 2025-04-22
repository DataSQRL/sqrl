package com.datasqrl.config;

import java.nio.file.Path;

public class SqrlConstants {

  public static final String BUILD_DIR_NAME = "build";
  public static final String DEPLOY_DIR_NAME = "deploy";
  public static final String PLAN_DIR = "plan";
  public static final Path PLAN_PATH = Path.of(BUILD_DIR_NAME, DEPLOY_DIR_NAME, PLAN_DIR);
  public static final String PACKAGE_JSON = "package.json";
  public static final Path DEFAULT_PACKAGE = Path.of(PACKAGE_JSON);
  public static final String FLINK_ASSETS_DIR = "flink";
  public static final String LIB_DIR = "lib";
  public static final String DATA_DIR = "data";
}
