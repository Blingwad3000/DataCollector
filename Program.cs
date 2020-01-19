using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataCollector
{
    public enum ApiFunction { Daily, ThirtyMin }
    public enum OutputSize { Full, Compact }

    public static class AlphaVantageConfigs
    {
        public static readonly string Folder;
        public static readonly string BaseUrl;
        public static readonly string BaseFileName;
        public static readonly string ConnectionString;

        static AlphaVantageConfigs()
        {
            Folder = System.Configuration.ConfigurationManager.AppSettings["folder"];
            BaseUrl = "https://www.alphavantage.co/query?function=@function&symbol=@symbol&apikey=@key&datatype=csv&outputsize=@outputsize<interval>";
            BaseFileName = "@baseFolder\\@symbol_@runDate_@outputsize_@function.csv"
                           .Replace("@baseFolder", Folder);
            ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["database"].ConnectionString;
        }

        internal static List<AlphaVantageAgent.KeyAgent> LoadKeys(AlphaVantageAgent parent)
        {
            List<AlphaVantageAgent.KeyAgent> keyAgents;

            string keyQuery = @"select ApiKey,
       coalesce(DateLastUsed, '1900-01-01') as DateLastUsed,
       coalesce(DailyCallCount, 0) as DailyCallCount,
	   DailyCallMax as DailyCallMax,
	   coalesce(CallsPerMinute, 0) as CallsPerMinute
from AlphaVantageKeys;";

            List<KeyDto> dtos = SQLUtilities.FromQuery<KeyDto>(keyQuery);

            keyAgents = dtos.Select(d => new AlphaVantageAgent.KeyAgent(parent, d.DailyCallMax, d.CallsPerMinute, d.ApiKey, d.DateLastUsed, d.DailyCallCount))
                            .ToList();

            return keyAgents;
        }
        
        public static string BuildUrl(OutputSize outputSize, ApiFunction apiFunction, string symbol, string key)
        {
            string url = BaseUrl;
            if (apiFunction == ApiFunction.Daily)
                url = url.Replace("<interval>", "");
            else if (apiFunction == ApiFunction.ThirtyMin)
                url = url.Replace("<interval>", "&interval=30min");
            
            return url.Replace("@function", func(apiFunction))
                      .Replace("@key", key)
                      .Replace("@outputsize", output(outputSize))
                      .Replace("@symbol", symbol);
        }

        public static string BuildFilename(OutputSize outputSize, ApiFunction apiFunction, string symbol)
        {
            return BaseFileName.Replace("@function", func(apiFunction))
                               .Replace("@runDate", DateTime.Today.AddDays(-1).ToShortDateString().Replace("/", "-"))
                               .Replace("@outputsize", output(outputSize))
                               .Replace("@symbol", symbol);
        }

        private static string output(OutputSize outputSize)
        {
            switch (outputSize)
            {
                case OutputSize.Full:
                    return "full";
                case OutputSize.Compact:
                    return "compact";
            }

            return "";
        }

        private static string func(ApiFunction type)
        {
            switch (type)
            {
                case ApiFunction.Daily:
                    return "TIME_SERIES_DAILY";
                case ApiFunction.ThirtyMin:
                    return "TIME_SERIES_INTRADAY";
            }

            return "";
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            AlphaVantageAgent agent = new AlphaVantageAgent();

            agent.Start();

            Console.WriteLine("bye!");
        }
    }
}
