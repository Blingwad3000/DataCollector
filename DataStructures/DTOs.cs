using System;

namespace DataCollector
{
    internal class KeyDto
    {
        public string ApiKey;
        public DateTime DateLastUsed;
        public int DailyCallCount;
        public int? DailyCallMax;
        public int CallsPerMinute;
    }
}
