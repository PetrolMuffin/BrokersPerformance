namespace Brokers.Performance.Helpers;

public static class TaskExtensions
{
    public static Task SafeAwait(this Task? task) => task ?? Task.CompletedTask;
}