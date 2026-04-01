namespace Brokers.Performance.Helpers;

public sealed class CounterCompletionSource
{
    private readonly int _completionCount;

    private volatile TaskCompletionSource<bool> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private volatile int _counter;
    
    public CounterCompletionSource(int completionCount)
    {
        _completionCount = completionCount;
    }

    public void Increment()
    {
        if (Interlocked.Increment(ref _counter) == _completionCount)
        {
            _tcs.SetResult(true);
        }
    }

    public async Task WhenCompleted(CancellationToken token)
    {
        await Task.WhenAny(_tcs.Task.WaitAsync(token), Task.Delay(TimeSpan.FromMinutes(2), token));
        if (!_tcs.Task.IsCompleted)
        {
            await Console.Out.WriteLineAsync("Timeout. Count: " + _counter + " CompletionCount: " + _completionCount);
        }
    }
}