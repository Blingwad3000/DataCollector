namespace DataCollector
{
    public interface ILoadTask
    {
        void Run(string key);

        string Identifier { get; }
        string Descriptor { get; }
    }
}
