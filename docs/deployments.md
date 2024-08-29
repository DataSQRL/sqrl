# Deployment

DataSQRL can target any deployment infrastructure through the concept of "deployment profiles". A deployment profile is a collection of deployment assets for each engine in the data pipeline/microservice that can be templated using [Freemarker templates](https://freemarker.apache.org/). The templates are instantiated with the output from the DataSQRL compiler to produce the deployment assets. Those deployment assets can then be deployed manually or through CI/CD pipelines and other automation tools.

DataSQRL provides default deployment profiles for its internal test and development runtime, Docker, and Kubernetes. Deployment profiles can also target Terraform or any other IaC (infrastructure-as-code) platform.

## Deployment Profiles Process

The deployment profile is configured in the projects [package.json](/docs/sqrl/datasqrl-spec) file. If the profile is not explicitly configured, DataSQRL's default Docker profile is used.

A deployment profile contains a root `package.json` configuration file that contains all the default configuration for the engines that the profile supports and default configuration for the compiler. That package.json is merged with the project's configuration file, with the latter taking precedence.

During compilation, DataSQRL produces physical plans for all configured engines in the targeted data pipeline/microservice topology as configured in the (potentially merged) project configuration files.
The physical plans are written as JSON documents to the `build/plan` directory with one file per engine with the name of the engine.

As the final step of the compilation process, the deployment assets from the deployment profile are copied to the `build/deploy` folder and all Freemarker templates (i.e. files ending in `.ftl`) are instantiated with the values from the physical plans for each engine.

Once the compilation completes, the deployment assets in the `build/deploy` folder are ready to be deployed or executed locally.

## Custom Deployment Values

The `values` section of the [DataSQRL configuration](/docs/sqrl/datasqrl-spec) allows you to specify configuration values that are passed through to the deployment profile and can be referenced in the deployment profile templates.

This allows you to specify runtime configuration in the project configuration file.

## Creating Deployment Profiles

Creating a custom deployment profile allows you to:

1. Target your specific data infrastructure
2. Customize the deployment to your needs
3. Standardize DataSQRL project development and deployment in your organization.

A deployment profile consists of:

* A `package.json` configuration file that is used as the basis for all DataSQRL projects that use this profile. This file is placed in the root of the deployment profile.
* A folder for each engine that is supported by the profile with engine specific deployment artifacts.
* Any other "shared" deployment assets that are used across the engines or that pull the individual engines together into one deployment (e.g. a docker compose template). Those are placed in the root of the deployment profile.

The templates are instantiated with the values from the physical plan documents and any values specified in the project configuration under `values`. In addition, the templating engine also provides these variables:
- `config`: the full package.json file
- `environment`: The current system environment.

When building your own deployment profile, it is best to start with an existing profile and iterate from there.
Take a look at the [default DataSQRL profiles](https://github.com/DataSQRL/sqrl/tree/main/profiles).

Deployment profiles can be merged. This is useful to overwrite the (templated) deployment assets for one engine without affecting the other engines. Deployment profiles are merged on a per-folder level.
Often it is sufficient to overwrite just one engine instead of defining a completely new deployment profile.
To use such incremental deployment profiles, you specify the `profiles` field in the `package.json` as an array of deployment profiles that contains the base/default deployment profile and the incremental one.

Deployment profiles can be uploaded to the repository and resolved as dependencies in DataSQRL projects with versioning for consistency, reuse, and collaboration.