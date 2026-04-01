using System.Collections.Concurrent;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Brokers.Performance.ReplyBenchmarks;

public class RabbitRequestReply : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private readonly string _requestQueue;
    private readonly string _replyQueue;
    private readonly byte[] _payload;

    private IConnection? _requesterConnection;
    private IChannel? _requesterPubChannel;
    private IChannel? _replySubChannel;
    private AsyncEventingBasicConsumer? _replyConsumer;
    private string? _replyConsumerTag;

    private IConnection? _responderConnection;
    private IChannel? _responderSubChannel;
    private IChannel? _responderPubChannel;
    private AsyncEventingBasicConsumer? _requestConsumer;
    private string? _requestConsumerTag;

    private readonly ConcurrentDictionary<string, TaskCompletionSource<byte[]>> _pending = new();

    public RabbitRequestReply(in byte[] payload)
    {
        var id = Guid.NewGuid().ToString("N")[..8];
        _requestQueue = $"bench_rr_req_{id}";
        _replyQueue = $"bench_rr_rep_{id}";
        _payload = payload;
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _cts.Dispose();

        if (_requestConsumer != null)
        {
            _requestConsumer.ReceivedAsync -= OnRequestReceived;
        }

        if (_replyConsumer != null)
        {
            _replyConsumer.ReceivedAsync -= OnReplyReceived;
        }

        if (_requestConsumerTag != null && _responderSubChannel != null)
        {
            await _responderSubChannel.BasicCancelAsync(_requestConsumerTag);
        }

        if (_replyConsumerTag != null && _replySubChannel != null)
        {
            await _replySubChannel.BasicCancelAsync(_replyConsumerTag);
        }

        if (_responderPubChannel != null)
        {
            await _responderPubChannel.CloseAsync();
            await _responderPubChannel.DisposeAsync();
        }

        if (_responderSubChannel != null)
        {
            await _responderSubChannel.CloseAsync();
            await _responderSubChannel.DisposeAsync();
        }

        if (_responderConnection != null)
        {
            await _responderConnection.CloseAsync();
            await _responderConnection.DisposeAsync();
        }

        if (_requesterPubChannel != null)
        {
            await _requesterPubChannel.QueueDeleteAsync(_requestQueue);
            await _requesterPubChannel.QueueDeleteAsync(_replyQueue);
            await _requesterPubChannel.CloseAsync();
            await _requesterPubChannel.DisposeAsync();
        }

        if (_replySubChannel != null)
        {
            await _replySubChannel.CloseAsync();
            await _replySubChannel.DisposeAsync();
        }

        if (_requesterConnection != null)
        {
            await _requesterConnection.CloseAsync();
            await _requesterConnection.DisposeAsync();
        }

        GC.SuppressFinalize(this);
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
                var properties = new BasicProperties
                {
                    CorrelationId = correlationId,
                    ReplyTo = _replyQueue
                };

                await _requesterPubChannel!.BasicPublishAsync("", _requestQueue, true, properties, _payload, token);
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
        var token = _cts.Token;
        var factory = new ConnectionFactory
        {
            HostName = Configuration.RabbitMqHost,
            Port = Configuration.RabbitMqPort,
            RequestedHeartbeat = TimeSpan.FromMinutes(2)
        };

        var channelOptions = new CreateChannelOptions(true, true);

        _requesterConnection = await factory.CreateConnectionAsync(token);
        _requesterPubChannel = await _requesterConnection.CreateChannelAsync(channelOptions, token);
        await _requesterPubChannel.QueueDeclareAsync(_requestQueue, true, false, false, cancellationToken: token);
        await _requesterPubChannel.QueueDeclareAsync(_replyQueue, true, false, false, cancellationToken: token);

        _replySubChannel = await _requesterConnection.CreateChannelAsync(channelOptions, token);
        await _replySubChannel.BasicQosAsync(0, 100, false, token);
        _replyConsumer = new AsyncEventingBasicConsumer(_replySubChannel);
        _replyConsumer.ReceivedAsync += OnReplyReceived;
        _replyConsumerTag = await _replySubChannel.BasicConsumeAsync(_replyQueue, false, _replyConsumer, token);

        _responderConnection = await factory.CreateConnectionAsync(token);
        _responderSubChannel = await _responderConnection.CreateChannelAsync(channelOptions, token);
        await _responderSubChannel.BasicQosAsync(0, 100, false, token);
        _requestConsumer = new AsyncEventingBasicConsumer(_responderSubChannel);
        _requestConsumer.ReceivedAsync += OnRequestReceived;
        _requestConsumerTag = await _responderSubChannel.BasicConsumeAsync(_requestQueue, false, _requestConsumer, token);

        _responderPubChannel = await _responderConnection.CreateChannelAsync(channelOptions, token);

        await Task.Delay(2000, token);
    }

    private async Task OnRequestReceived(object sender, BasicDeliverEventArgs args)
    {
        await _responderSubChannel!.BasicAckAsync(args.DeliveryTag, multiple: false);

        var properties = new BasicProperties { CorrelationId = args.BasicProperties.CorrelationId };
        await _responderPubChannel!.BasicPublishAsync("", args.BasicProperties.ReplyTo!, true, properties, _payload);
    }

    private async Task OnReplyReceived(object sender, BasicDeliverEventArgs args)
    {
        await _replySubChannel!.BasicAckAsync(args.DeliveryTag, multiple: false);

        var correlationId = args.BasicProperties.CorrelationId;
        if (correlationId != null && _pending.TryGetValue(correlationId, out var tcs))
        {
            tcs.TrySetResult(args.Body.ToArray());
        }
    }
}