using BenchmarkDotNet.Attributes;
using Brokers.Performance.Helpers;

namespace Brokers.Performance.ReplyBenchmarks;

[MemoryDiagnoser]
public class Benchmarks
{
    private const int Publishers = 150;

    private RabbitRequestReply? _rabbit;
    private KafkaRequestReply? _kafka;
    private NatsRequestReply? _nats;

    public static (int PushCount, int PayloadSize)[] Params() =>
    [
        (25000, 256),
        (10000, 1024),
        (5000, 4 * 1024),
    ];

    [ParamsSource(nameof(Params))]
    public (int PushCount, int PayloadBytes) Current;

    [IterationCleanup]
    public void IterationCleanup()
    {
        _rabbit?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _kafka?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _nats?.DisposeAsync().AsTask().GetAwaiter().GetResult();

        _rabbit = null;
        _kafka = null;
        _nats = null;
    }

    [GlobalSetup]
    public void Setup() => PayloadProvider.Preload(Params().Select(p => p.PayloadSize));

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_rabbit != null) await _rabbit.DisposeAsync();
        if (_kafka != null) await _kafka.DisposeAsync();
        if (_nats != null) await _nats.DisposeAsync();

        _rabbit = null;
        _kafka = null;
        _nats = null;
    }

    [IterationSetup(Target = nameof(Rabbit))]
    public void SetupRabbit()
    {
        if (_rabbit != null) return;

        _rabbit = new RabbitRequestReply(PayloadProvider.GetPayload(Current.PayloadBytes));
        _rabbit.Setup().GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task Rabbit() => await _rabbit!.Invoke(Current.PushCount, Publishers);

    [IterationSetup(Target = nameof(Kafka))]
    public void SetupKafka()
    {
        if (_kafka != null) return;

        _kafka = new KafkaRequestReply(PayloadProvider.GetPayload(Current.PayloadBytes));
        _kafka.Setup().GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task Kafka() => await _kafka!.Invoke(Current.PushCount, Publishers);

    [IterationSetup(Target = nameof(Nats))]
    public void SetupNats()
    {
        if (_nats != null) return;

        _nats = new NatsRequestReply(PayloadProvider.GetPayload(Current.PayloadBytes));
        _nats.Setup().GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true)]
    public async Task Nats() => await _nats!.Invoke(Current.PushCount, Publishers);
}