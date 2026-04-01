using BenchmarkDotNet.Attributes;
using Brokers.Performance.Helpers;

namespace Brokers.Performance.AsyncBenchmarks;

[MemoryDiagnoser]
public class Benchmarks
{
    private const int Publishers = 250;
    
    private RabbitProdCon? _rabbitProdCon;
    private KafkaProdCon? _kafkaProdCon;
    private NatsJsProdCon? _natsJs;

    public static (int PushCount, int PayloadSize)[] Params() =>
    [
        (50000, 256),
        (25000, 1024),
        (10000, 4 * 1024),
        (5000, 64 * 1024),
        (2500, 128 * 1024)
    ];

    [ParamsSource(nameof(Params))]
    public (int PushCount, int PayloadBytes) Current;

    [IterationCleanup]
    public void IterationCleanup()
    {
        _rabbitProdCon?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _kafkaProdCon?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _natsJs?.DisposeAsync().AsTask().GetAwaiter().GetResult();

        _rabbitProdCon = null;
        _kafkaProdCon = null;
        _natsJs = null;
    }

    [GlobalSetup]
    public void Setup() => PayloadProvider.Preload(Params().Select(p => p.PayloadSize));

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (_rabbitProdCon != null) await _rabbitProdCon.DisposeAsync();
        if (_kafkaProdCon != null) await _kafkaProdCon.DisposeAsync();
        if (_natsJs != null) await _natsJs.DisposeAsync();

        _rabbitProdCon = null;
        _kafkaProdCon = null;
        _natsJs = null;
    }

    [IterationSetup(Target = nameof(Rabbit))]
    public void SetupRabbit()
    {
        if (_rabbitProdCon != null) return;

        _rabbitProdCon = new RabbitProdCon(PayloadProvider.GetPayload(Current.PayloadBytes));
        _rabbitProdCon.Setup().GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task Rabbit() => await _rabbitProdCon!.Invoke(Current.PushCount, Publishers);

    [IterationSetup(Target = nameof(Kafka))]
    public void SetupKafka()
    {
        if (_kafkaProdCon != null) return;

        _kafkaProdCon = new KafkaProdCon(PayloadProvider.GetPayload(Current.PayloadBytes));
        _kafkaProdCon.Setup().GetAwaiter().GetResult();
    }

    [Benchmark]
    public async Task Kafka() => await _kafkaProdCon!.Invoke(Current.PushCount, Publishers);

    [IterationSetup(Target = nameof(NatsJs))]
    public void SetupNatsJs()
    {
        if (_natsJs != null) return;

        _natsJs = new NatsJsProdCon(PayloadProvider.GetPayload(Current.PayloadBytes));
        _natsJs.Setup().GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true)]
    public async Task NatsJs() => await _natsJs!.Invoke(Current.PushCount, Publishers);
}