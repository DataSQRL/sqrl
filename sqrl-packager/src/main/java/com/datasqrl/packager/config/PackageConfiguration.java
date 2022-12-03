package com.datasqrl.packager.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@JsonIgnoreProperties(ignoreUnknown=true)
public class PackageConfiguration {

    String name;
    String version;

    String variant;
    String license;
    String repository;
    String homepage;
    String documentation;
    String readme;
    String description;

}
