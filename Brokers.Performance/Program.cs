using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using ReplyBenchmarks = Brokers.Performance.ReplyBenchmarks;
using AsyncBenchmarks = Brokers.Performance.AsyncBenchmarks;

const int warmupCount = 3;
const int iterationCount = 10;
const int unrollFactor = 1;
const int invocationCount = 1;

var job = Job.Default.WithInvocationCount(invocationCount)
                     .WithUnrollFactor(unrollFactor)
                     .WithWarmupCount(warmupCount)
                     .WithIterationCount(iterationCount)
                     .WithGcConcurrent(false)
                     .WithGcForce(true)
                     .WithGcServer(false)
                     .WithStrategy(RunStrategy.Monitoring);

var config = ManualConfig.Create(DefaultConfig.Instance)
                         .AddColumn(StatisticColumn.Median)
                         .AddColumn(StatisticColumn.P95)
                         .AddColumn(StatisticColumn.OperationsPerSecond)
                         .AddJob(job);

ThreadPool.SetMinThreads(100, 100);

BenchmarkRunner.Run<ReplyBenchmarks.Benchmarks>(config);
BenchmarkRunner.Run<AsyncBenchmarks.Benchmarks>(config);