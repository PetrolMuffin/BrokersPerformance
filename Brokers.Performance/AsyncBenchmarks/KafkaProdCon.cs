using Brokers.Performance.Helpers;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Brokers.Performance.AsyncBenchmarks;

public sealed class KafkaProdCon : IAsyncDisposable
{
    private readonly string _topic;
    private readonly byte[] _payload;
    private readonly CancellationTokenSource _cts = new();

    private readonly IProducer<Null, byte[]> _producer;
    private IConsumer<Null, byte[]>? _consumer;

    private Task? _consumerTask;
    private CounterCompletionSource? _receivedBatchTcs;

    public KafkaProdCon(in byte[] payload)
    {
        _payload = payload;
        _topic = $"bench-pc-{Guid.NewGuid():N}";

        var bootstrap = Configuration.KafkaBootstrapServers;
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            EnableIdempotence = true,
            QueueBufferingMaxKbytes = 1024 * 1024 * 1024,
            QueueBufferingMaxMessages = 1000000
        };

        _producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        await _consumerTask.SafeAwait();
        _cts.Dispose();

        _producer.Dispose();

        _consumer?.Close();
        _consumer?.Dispose();
    }

    public async Task Invoke(int pushCount, int writers)
    {
        // reset state
        var tcs = _receivedBatchTcs = new CounterCompletionSource(pushCount);

        var opts = new ParallelOptions
        {
            CancellationToken = _cts.Token,
            MaxDegreeOfParallelism = writers
        };

        await Parallel.ForAsync(0, pushCount, opts, async (_, token) =>
        {
            await _producer.ProduceAsync(_topic, new Message<Null, byte[]> { Value = _payload }, token);
        });

        // wait for the batch to be received
        await tcs.WhenCompleted(_cts.Token);
    }

    public async Task Setup()
    {
        var bootstrap = Configuration.KafkaBootstrapServers;
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrap };
        using (var admin = new AdminClientBuilder(adminConfig).Build())
        {
            var topicSpec = new TopicSpecification
            {
                Name = _topic,
                NumPartitions = 1,
                ReplicationFactor = 1
            };

            await admin.CreateTopicsAsync([topicSpec]);
        }

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = Guid.NewGuid().ToString(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build();
        _consumer.Subscribe(_topic);

        var token = _cts.Token;
        _consumerTask = Task.Run(() =>
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var cr = _consumer.Consume(token);
                    _consumer.Commit(cr);
                    _receivedBatchTcs!.Increment();
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }, token);

        await Task.Delay(2000, token);
    }
}