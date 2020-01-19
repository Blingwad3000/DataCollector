using System;

namespace DataCollector
{
    public class StockSymbol
    {
        public string   Symbol;
        public string   Exchange;
        public string   CompanyName;
        public bool     HistoricalLoadStatus;
        public bool     HourlyFullStatus;
        public DateTime MinDate;
    }
}
