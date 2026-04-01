using System.Collections.Concurrent;
using System.Text;
using Brokers.Performance.Helpers;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Brokers.Performance.ReplyBenchmarks;

public sealed class KafkaRequestReply : IAsyncDisposable
{
    private readonly string _requestTopic;
    private readonly string _replyTopic;
    private readonly byte[] _payload;
    private readonly CancellationTokenSource _cts = new();

    private readonly IProducer<Null, byte[]> _requesterProducer;
    private readonly IProducer<Null, byte[]> _responderProducer;
    private IConsumer<Null, byte[]>? _responderConsumer;
    private IConsumer<Null, byte[]>? _replyConsumer;

    private Task? _responderTask;
    private Task? _replyListenerTask;

    private readonly ConcurrentDictionary<string, TaskCompletionSource<byte[]>> _pending = new();

    public KafkaRequestReply(in byte[] payload)
    {
        _payload = payload;

        var id = Guid.NewGuid().ToString("N")[..8];
        _requestTopic = $"bench-rr-req-{id}";
        _replyTopic = $"bench-rr-rep-{id}";

        var bootstrap = Configuration.KafkaBootstrapServers;
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            EnableIdempotence = true,
        };

        _requesterProducer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
        _responderProducer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        await _responderTask.SafeAwait();
        await _replyListenerTask.SafeAwait();
        _cts.Dispose();

        _requesterProducer.Dispose();
        _responderProducer.Dispose();

        _responderConsumer?.Close();
        _responderConsumer?.Dispose();

        _replyConsumer?.Close();
        _replyConsumer?.Dispose();
    }

    public async Task Invoke(int requestCount, int writers)
    {
        var opts = new ParallelOptions
        {
            CancellationToken = _cts.Token,
            MaxDegreeOfParallelism = writers
        };

        var completionTimeout = TimeSpan.FromMinutes(1);
        await Parallel.ForAsync(0, requestCount, opts, async (i, token) =>
        {
            var correlationId = $"{i}-{Guid.NewGuid():N}";
            var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pending[correlationId] = tcs;

            try
            {
                var headers = new Headers { { "correlation-id", Encoding.UTF8.GetBytes(correlationId) } };
                var message = new Message<Null, byte[]>
                {
                    Value = _payload,
                    Headers = headers
                };

                await _requesterProducer.ProduceAsync(_requestTopic, message, token);
                await tcs.Task.WaitAsync(completionTimeout, token);
            }
            catch (TimeoutException)
            {
                Console.WriteLine($"Timeout for request {i}");
            }
            finally
            {
                _pending.TryRemove(correlationId, out _);
            }
        });
    }

    public async Task Setup()
    {
        var bootstrap = Configuration.KafkaBootstrapServers;
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrap };
        using (var admin = new AdminClientBuilder(adminConfig).Build())
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = _requestTopic,
                    NumPartitions = 1,
                    ReplicationFactor = 1
                },
                new TopicSpecification
                {
                    Name = _replyTopic,
                    NumPartitions = 1,
                    ReplicationFactor = 1
                }
            ]);
        }

        var token = _cts.Token;

        // Responder: consumes requests and produces replies
        var responderConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"responder-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _responderConsumer = new ConsumerBuilder<Null, byte[]>(responderConsumerConfig).Build();
        _responderConsumer.Subscribe(_requestTopic);

        _responderTask = Task.Run(() =>
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var result = _responderConsumer.Consume(token);
                    var corrHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "correlation-id") ?? throw new Exception("No correlation id");
                    var replyHeaders = new Headers { { "correlation-id", corrHeader.GetValueBytes() } };
                    var reply = new Message<Null, byte[]>
                    {
                        Value = _payload,
                        Headers = replyHeaders
                    };

                    _responderProducer.Produce(_replyTopic, reply);
                    _responderConsumer.Commit(result);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }, token);

        // Reply listener: consumes replies and completes pending requests
        var replyConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"reply-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _replyConsumer = new ConsumerBuilder<Null, byte[]>(replyConsumerConfig).Build();
        _replyConsumer.Subscribe(_replyTopic);

        _replyListenerTask = Task.Run(() =>
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var result = _replyConsumer.Consume(token);
                    var corrHeader = result.Message.Headers.FirstOrDefault(h => h.Key == "correlation-id") ?? throw new Exception("No correlation id");
                    var correlationId = Encoding.UTF8.GetString(corrHeader.GetValueBytes());
                    if (_pending.TryGetValue(correlationId, out var tcs))
                    {
                        tcs.TrySetResult(result.Message.Value);
                    }

                    _replyConsumer.Commit(result);
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