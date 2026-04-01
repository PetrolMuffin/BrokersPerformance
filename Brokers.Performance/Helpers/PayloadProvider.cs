namespace Brokers.Performance.Helpers;

public static class PayloadProvider
{
    private static readonly Dictionary<int, byte[]> Payloads = new();

    public static byte[] GetPayload(int size) => Payloads[size];

    public static void Preload(IEnumerable<int> packages)
    {
        foreach (var size in packages)
        {
            Payloads[size] = Enumerable.Repeat((byte)'a', size).ToArray();
        }
    }
}