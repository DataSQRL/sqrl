# Default Profile

The default profile for DataSQRL is automatically selected if no other profile is provided. This
profile supports all versions past 0.5.3 and provides a Docker Compose setup for PostgreSQL, Flink,
Kafka, and Vert.x HTTP server.

## Configuration Options

This profile offers configuration options for several engines it uses.

### Kafka

If additional Kafka topics are required, they can be added to your package.json with
the `create-topics` value. These topics will be created automatically.

```json
{
  "values": {
    "create-topics": [
      "topic1",
      "topic2"
    ]
  }
}
```

In the *clickstream-ai-recommendation* example, you can find
an [example](https://github.com/DataSQRL/datasqrl-examples/blob/a7da9067c3c4a95c950ce1a3a91b5e3d7e6ef5fa/clickstream-ai-recommendation/package.json#L12)
where we create additional topics manually.

### Flink

Additional Flink configurations can be provided with the `flink-config` value. This allows you to
pass standard Flink configurations. For instance, to configure checkpointing, you might need to
specify `execution.checkpointing.interval`, `state.backend`, and `state.checkpoints.dir`. Here's how
you can provide them using this profile:

```json
{
  "values": {
    "flink-config": {
      "execution.checkpointing.interval": "10 min",
      "state.backend": "filesystem",
      "state.checkpoints.dir": "file://<checkpoints-dir>"
    }
  }
}
```

It is common to use additional file resources in the pipeline. For this, the profile provides
a `mountDir` configuration option to mount additional resources. These resources will be mounted in
the Flink job- and task-manager containers using Docker Compose `volumes` syntax.

```json
{
  "values": {
    "mountDir": "/mylocaldir:/myremotedir"
  }
}
```

In the *clickstream-ai-recommendation* example, you can find
an [example](https://github.com/DataSQRL/datasqrl-examples/blob/a7da9067c3c4a95c950ce1a3a91b5e3d7e6ef5fa/clickstream-ai-recommendation/package.json#L11)
of how to specify a mount point.

### Enabled Engines

This profile enables the `vertx`, `postgres`, `kafka`, and `flink` engines. However, it is possible
to disable some of these engines based on your use case. To disable Vert.x (e.g., if you don't want
to execute GraphQL queries), simply omit it from the `enabled-engines` list:

```json
{
  "enabled-engines": ["postgres", "kafka", "flink"]
}
```

## Example Usage

### Basic Scenario

This profile is automatically selected if no other profile is provided. In
the [logistics-shipping-geodata](https://github.com/DataSQRL/datasqrl-examples/blob/main/logistics-shipping-geodata)
example, it is used automatically.

### Advanced Scenario

It is a good practice to use the default profile as a base profile that can be extended or
overridden. For instance, if you want to use Redpanda instead of Kafka while retaining the other
configurations from the default profile, you can do so.

To use this profile, add it to the profiles list in your project's package.json:

```json
{
  "profiles": [
    "datasqrl.profile.default"
  ]
}
```

It is also recommended to specify the exact version used in the dependencies section:

```json
{
  "dependencies": [
    {
      "datasqrl.profile.default": {
        "name": "datasqrl.profile.default",
        "version": "0.5.4",
        "variant": "dev"
      }
    }
  ]
}
```

When swapping out an existing service, you need to respect the structure of the default profile.
Other services will reference the log engine as Kafka, so you need to keep that name. To replace
Kafka with Redpanda, modify the `kafka` folder and the `kafka.compose.yml`. The content in these
will override the default profile.

For this use case, see
the [clickstream-ai-recommendation](https://github.com/DataSQRL/datasqrl-examples/blob/a7da9067c3c4a95c950ce1a3a91b5e3d7e6ef5fa/clickstream-ai-recommendation/package.json#L8)
example.

#### Good to Know:

- You need to specify the enabled profiles in order. The latter in the list will override the
  respective parts of the previous profiles.
- SQRL doesn't support merging at the engine level. Therefore, the `vertx`, `postgres`, `kafka`,
  and `flink` folders will be fully overridden if specified in subsequent profiles.
