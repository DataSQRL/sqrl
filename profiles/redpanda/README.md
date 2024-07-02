# Redpanda Profile

The Redpanda profile for DataSQRL allows you to use Redpanda as an engine instead of Kafka, and you
can use it in conjunction with the default profile.

## Configuration Options

This profile offers configuration options for the Redpanda engine it uses.

If additional Redpanda topics are required, they can be added to your package.json with
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

### Configuring it as an Engine

As described in the [Default Profile's Advanced Scenario](../default/README.md#advanced-scenario)
section, we need to respect the original structure of the default profile. Since the Redpanda
profile follows that rule, the only thing needed is to configure it as a profile in the profile
section.

## Example Usage

This profile can be selected explicitly to use Redpanda instead of Kafka. In
the [clickstream-ai-recommendation](https://github.com/DataSQRL/datasqrl-examples/blob/main/clickstream-ai-recommendation)
example you can see how to do it:

```json
{
  "profiles": [
    "datasqrl.profile.default",
    "datasqrl.profile.redpanda"
  ]
}
```

It is also recommended to specify the exact version used in the dependencies section:

```json
{
  "dependencies": [
    {
      "datasqrl.profile.redpanda": {
        "name": "datasqrl.profile.redpanda",
        "version": "0.5.2",
        "variant": "dev"
      }
    }
  ]
}
```
