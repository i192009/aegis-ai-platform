# KEDA autoscaling

The Helm chart renders two `ScaledObject` resources:

- evaluation workers scale from RabbitMQ queue depth;
- audit consumers scale from Kafka consumer lag.

Kafka useful parallelism cannot exceed the topic partition count. Increasing replicas beyond partitions creates idle consumers, so production partition planning must consider peak tenant-keyed throughput before raising `maxReplicaCount`.
