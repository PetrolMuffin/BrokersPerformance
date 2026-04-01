using System.Threading.Channels;
using Brokers.Performance.Helpers;
using NATS.Client.Core;

namespace Brokers.Performance.ReplyBenchmarks;

public class NatsRequestReply : IAsyncDisposable
{
    private readonly NatsConnection _requesterConnection;
    private readonly NatsConnection _responderConnection;
    private readonly CancellationTokenSource _cts = new();

    private Task? _responderTask;

    private readonly byte[] _payload;
    private readonly string _subject;

    public NatsRequestReply(in byte[] payload)
    {
        _payload = payload;

        var subjectId = Guid.NewGuid().ToString("N")[..8];
        _subject = $"bench.rr.{subjectId}";

        var opts = NatsOpts.Default with
        {
            Url = Configuration.NatsUrl,
            RequestTimeout = TimeSpan.FromMinutes(10),
            SubPendingChannelFullMode = BoundedChannelFullMode.Wait,
            WriterBufferSize = 1024 * 1024,
            CommandTimeout = TimeSpan.FromMinutes(5),
            MaxReconnectRetry = 10
        };

        _requesterConnection = new NatsConnection(opts);
        _responderConnection = new NatsConnection(opts);
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        await _responderTask.SafeAwait();
        _cts.Dispose();

        await _responderConnection.DisposeAsync();
        await _requesterConnection.DisposeAsync();

        GC.SuppressFinalize(this);
    }

    public async Task Invoke(int requestCount, int writers)
    {
        var opts = new ParallelOptions
        {
            CancellationToken = _cts.Token,
            MaxDegreeOfParallelism = writers
        };

        await Parallel.ForAsync(0, requestCount, opts,
                                async (_, token) => await _requesterConnection.RequestAsync<byte[], byte[]>(_subject, _payload, cancellationToken: token));
    }

    public async Task Setup()
    {
        var token = _cts.Token;

        _responderTask = Task.Run(async () =>
        {
            await foreach (var msg in _responderConnection.SubscribeAsync<byte[]>(_subject, cancellationToken: token))
            {
                await msg.ReplyAsync(_payload, cancellationToken: token);
            }
        }, token);

        await Task.Delay(2000, token);
    }
}