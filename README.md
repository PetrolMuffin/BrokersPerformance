# Brokers.Performance

Throughput benchmarks for three message brokers on .NET 10:

- **RabbitMQ 4.2** (AMQP, `rabbitmq:4.2-management`)
- **Apache Kafka 4.2** (KRaft mode, `apache/kafka:4.2.0`)
- **NATS 2.12 with JetStream** (`nats:2.12-alpine`)

Two messaging patterns tested:

- **Async queue** (producer-consumer): 250 concurrent publishers, 1 consumer, payloads from 256 B to 128 KB
- **Request-reply**: 150 concurrent publishers, payloads from 256 B to 4 KB

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Docker](https://www.docker.com/) with Docker Compose

## Running

Start the brokers:

```bash
docker compose up -d
```

Wait for all containers to become healthy, then run benchmarks:

```bash
cd Brokers.Performance
dotnet run -c Release
```

Results will appear in `BenchmarkDotNet.Artifacts/results/`.

## Project Structure

```
Brokers.Performance/
├── AsyncBenchmarks/          # Producer-consumer pattern
│   ├── Benchmarks.cs         # BenchmarkDotNet harness (250 publishers)
│   ├── RabbitProdCon.cs      # RabbitMQ implementation
│   ├── KafkaProdCon.cs       # Kafka implementation
│   └── NatsJsProdCon.cs      # NATS JetStream implementation
├── ReplyBenchmarks/          # Request-reply pattern
│   ├── Benchmarks.cs         # BenchmarkDotNet harness (150 publishers)
│   ├── RabbitRequestReply.cs # RabbitMQ (correlation-ID emulation)
│   ├── KafkaRequestReply.cs  # Kafka (correlation-ID emulation)
│   └── NatsRequestReply.cs   # NATS (native RequestAsync)
├── Helpers/
│   ├── CounterCompletionSource.cs
│   ├── PayloadProvider.cs
│   └── TaskExtensions.cs
└── Program.cs                # BenchmarkDotNet config
```

## Configuration

All brokers run with default settings via `docker-compose.yml`. No custom tuning is applied.

BenchmarkDotNet config (in `Program.cs`):
- 3 warmup iterations, 10 measured iterations
- Non-concurrent GC, forced collections
- Monitoring strategy, 100 min threads
- Metrics: Mean, Median, P95, Op/s, memory allocation

## License

MIT
