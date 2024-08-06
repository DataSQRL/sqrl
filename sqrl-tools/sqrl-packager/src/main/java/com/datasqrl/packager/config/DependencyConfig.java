//package com.datasqrl.packager.config;
//
//import java.util.LinkedHashMap;
//import lombok.AllArgsConstructor;
//import lombok.NonNull;
//
//@AllArgsConstructor
//public class DependencyConfig {
//
//  public static final String DEPENDENCIES_KEY = "dependencies";
//  public static final String PKG_NAME_KEY = "pkgName";
//  public static final String VERSION_KEY = "version";
//  public static final String VARIANT_KEY = "variant";
//
//  SqrlConfig sqrlConfig;
//
//  public static LinkedHashMap<String,Dependency> fromRootConfig(@NonNull SqrlConfig config) {
//    return config.asMap(DEPENDENCIES_KEY,Dependency.class).get();
//  }
//
//}
