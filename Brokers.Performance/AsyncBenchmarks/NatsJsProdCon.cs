using Brokers.Performance.Helpers;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Brokers.Performance.AsyncBenchmarks;

public sealed class NatsJsProdCon : IAsyncDisposable
{
    private static readonly TimeSpan DuplicateWindow = TimeSpan.FromMinutes(1);
    private readonly string _streamName;

    private readonly NatsConnection _pubConnection;
    private readonly NatsJSContext _pubJs;

    private readonly NatsConnection _subConnection;
    private readonly NatsJSContext _subJs;
    private readonly CancellationTokenSource _cts = new();

    private Task? _consumerTask;
    private CounterCompletionSource? _receivedBatchTcs;

    private readonly byte[] _payload;
    private readonly string _subject;

    public NatsJsProdCon(in byte[] payload)
    {
        _payload = payload;

        _streamName = $"BENCH{Guid.NewGuid():N}"[..20].ToUpperInvariant();

        var subjectId = Guid.NewGuid().ToString("N")[..8];
        _subject = $"bench{subjectId}.push";

        var opts = NatsOpts.Default with
        {
            Url = Configuration.NatsUrl,
            WriterBufferSize = 1024 * 1024 * 1024,
            MaxReconnectRetry = 10,
            ConnectTimeout = TimeSpan.FromSeconds(5)
        };

        var jsOpts = new NatsJSOpts(opts);
        _pubConnection = new NatsConnection(opts);
        _pubJs = new NatsJSContext(_pubConnection, jsOpts);

        _subConnection = new NatsConnection(opts);
        _subJs = new NatsJSContext(_subConnection, jsOpts);
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        await _consumerTask.SafeAwait();
        _cts.Dispose();

        await _pubJs.DeleteStreamAsync(_streamName);

        await _subConnection.DisposeAsync();
        await _pubConnection.DisposeAsync();
    }

    public async Task Invoke(int pushCount, int writers)
    {
        var tcs = _receivedBatchTcs = new CounterCompletionSource(pushCount);

        var opts = new ParallelOptions
        {
            CancellationToken = _cts.Token,
            MaxDegreeOfParallelism = writers
        };
        
        await Parallel.ForAsync(0, pushCount, opts, async (_, token) =>
        {
            var headers = new NatsHeaders { { "Nats-Msg-ID", Guid.NewGuid().ToString() } };
            await _pubJs.PublishAsync(_subject, _payload, headers: headers, cancellationToken: token);
        });

        await tcs.WhenCompleted(_cts.Token);
    }

    public async Task Setup()
    {
        var token = _cts.Token;

        var streamConfig = new StreamConfig(_streamName, [_subject])
        {
            DuplicateWindow = DuplicateWindow,
            Discard = StreamConfigDiscard.New,
            Retention = StreamConfigRetention.Workqueue,
            PersistMode = StreamConfigPersistMode.Async,
            MaxBytes = -1,
            MaxMsgs = -1,
            Storage = StreamConfigStorage.File
        };

        await _pubJs.CreateOrUpdateStreamAsync(streamConfig, token);

        var consumerName = $"cons{Guid.NewGuid():N}"[..16];
        var consumerConfig = new ConsumerConfig(consumerName)
        {
            FilterSubject = _subject,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            MaxDeliver = 10
        };

        var consumer = await _subJs.CreateOrUpdateConsumerAsync(_streamName, consumerConfig, token);

        _consumerTask = Task.Run(async () =>
        {
            await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: token))
            {
                await msg.AckAsync(cancellationToken: token);
                _receivedBatchTcs!.Increment();
            }
        }, token);

        await Task.Delay(2000, token);
    }
}