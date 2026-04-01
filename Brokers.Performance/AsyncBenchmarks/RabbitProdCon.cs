using System.Diagnostics.CodeAnalysis;
using Brokers.Performance.Helpers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Brokers.Performance.AsyncBenchmarks;

public sealed class RabbitProdCon : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private readonly string _queueName;
    private readonly byte[] _payload;

    private IConnection? _pubConnection;
    private IChannel? _pubChannel;

    private IConnection? _subConnection;
    private IChannel? _subChannel;

    private string? _consumerTag;
    private AsyncEventingBasicConsumer? _consumer;

    private CounterCompletionSource? _receivedBatchTcs;

    public RabbitProdCon(in byte[] payload)
    {
        _queueName = $"bench_pc_{Guid.NewGuid():N}";
        _payload = payload;
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _cts.Dispose();

        if (!IsInitialized()) return;

        _consumer.ReceivedAsync -= OnReceived;

        await _subChannel!.BasicCancelAsync(_consumerTag);
        await _subChannel.CloseAsync();
        await _subChannel.DisposeAsync();

        await _subConnection!.CloseAsync();
        await _subConnection.DisposeAsync();

        await _pubChannel.QueueDeleteAsync(_queueName);
        await _pubChannel!.CloseAsync();
        await _pubChannel.DisposeAsync();

        await _pubConnection!.CloseAsync();
        await _pubConnection.DisposeAsync();
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
        
        var properties = new BasicProperties { Persistent = true };
        await Parallel.ForAsync(0, pushCount, opts, async (_, token) =>
        {
            await _pubChannel!.BasicPublishAsync("", _queueName, true, properties, _payload, token);
        });

        // wait for the batch to be received
        await tcs.WhenCompleted(_cts.Token);
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

        _pubConnection = await factory.CreateConnectionAsync(token);

        var channelOptions = new CreateChannelOptions(true, true);
        _pubChannel = await _pubConnection.CreateChannelAsync(channelOptions, token);
        await _pubChannel.QueueDeclareAsync(_queueName, true, false, false, cancellationToken: token);

        _subConnection = await factory.CreateConnectionAsync(token);
        _subChannel = await _subConnection.CreateChannelAsync(channelOptions, token);
        await _subChannel.BasicQosAsync(0, 100, false, token);
        
        _consumer = new AsyncEventingBasicConsumer(_subChannel);
        _consumer.ReceivedAsync += OnReceived;
        _consumerTag = await _subChannel.BasicConsumeAsync(_queueName, false, _consumer, token);

        await Task.Delay(2000, token);
    }

    private async Task OnReceived(object _, BasicDeliverEventArgs args)
    {
        await _subChannel!.BasicAckAsync(args.DeliveryTag, multiple: true);
        _receivedBatchTcs!.Increment();
    }

    [MemberNotNullWhen(true, nameof(_pubConnection), nameof(_pubChannel), nameof(_subConnection), nameof(_subChannel), nameof(_consumer), nameof(_consumerTag))]
    private bool IsInitialized() => _pubConnection != null && _pubChannel != null && _subConnection != null && _subChannel != null && _consumer != null && _consumerTag != null;
}