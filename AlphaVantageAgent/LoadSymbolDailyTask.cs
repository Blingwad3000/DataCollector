using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net;

namespace DataCollector
{
    public class LoadSymbolPriceData : ILoadTask
    {
        public string TargetSymbol { get; private set; }
        public OutputSize OutputSize { get; private set; }
        public ApiFunction ApiFunction { get; private set; }

        private readonly string configFolder;
        
        public string Identifier { get { return TargetSymbol + '-' + ((int)OutputSize).ToString() + '-' + ((int)ApiFunction).ToString(); } }
        public string Descriptor { get { return TargetSymbol + ", " + OutputSize.ToString() + ", " + this.ApiFunction.ToString(); } }

        public LoadSymbolPriceData(string stockSymbol, OutputSize outputSize, ApiFunction apiFunction)
        {
            this.OutputSize = outputSize;
            this.TargetSymbol = stockSymbol;
            this.ApiFunction = apiFunction;
        }
        
        public void Run(string key)
        {
            string fileName;
            
            using (var client = new WebClient())
            {
                fileName = AlphaVantageConfigs.BuildFilename(this.OutputSize, this.ApiFunction, this.TargetSymbol);
                string url = AlphaVantageConfigs.BuildUrl(this.OutputSize, this.ApiFunction, this.TargetSymbol, key);
                
                Console.WriteLine("key agent {2} requesting file {0} from {1}", fileName, url, key);
                client.DownloadFile(url, fileName);
            }
            
            List<StockDataPoint> data = new List<StockDataPoint>();

            try
            {
                using (TextFieldParser csvParser = new TextFieldParser(fileName))
                {
                    csvParser.SetDelimiters(new string[] { "," });
                    csvParser.ReadLine();

                    int rowsLoaded = 0;
                    while (!csvParser.EndOfData)
                    {
                        string[] fields = csvParser.ReadFields();

                        data.Add(new StockDataPoint(fields));
                        rowsLoaded += 1;
                    }
                    Console.WriteLine("key agent {0} loaded {1} rows", key, rowsLoaded);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            DataTable table = SQLUtilities.ToDataTable<StockDataPoint>(data);
            
            using (SqlConnection connection = new SqlConnection(AlphaVantageConfigs.ConnectionString))
            {
                connection.Open();
                
                string tempTableName = "#StockData";
                
                StockDataPoint.SqlCreateTempTable(connection, tempTableName);
                
                SqlBulkCopy sb = new SqlBulkCopy(connection);
                
                sb.DestinationTableName = tempTableName;
                
                sb.WriteToServer(table);
                
                string updateScript = "";
                string insertScript = "";

                if (this.ApiFunction == ApiFunction.Daily && this.OutputSize == OutputSize.Full)
                    updateScript = $@"update s
set s.HistoricalLoadStatus = 1,
    s.MinDate = t.MinDate
from StockSymbol s
     inner join (select Symbol, min(PriceDate) as MinDate
	             from StockPriceDaily ss
				 where ss.Symbol = '{this.TargetSymbol}'
				 group by Symbol) t
	     on s.Symbol = t.Symbol
where s.Symbol = '{this.TargetSymbol}';";
                else if (this.ApiFunction == ApiFunction.ThirtyMin && this.OutputSize == OutputSize.Full)
                    updateScript = $@"update s
set s.HalfHourlyFullStatus = 1,
    s.MinDateTime = t.MinDateTime
from StockSymbol s
     inner join (select Symbol, min(PriceDate) as MinDateTime
	             from StockPrice30Min ss
				 where ss.Symbol = '{this.TargetSymbol}'
				 group by Symbol) t
	     on s.Symbol = t.Symbol
where s.Symbol = '{this.TargetSymbol}';";

                insertScript = $@"insert into <Table> with (tablock)
(Symbol, PriceDate, [Open], High, Low, [Close], Volume)
select '{this.TargetSymbol}', [Open], [High], [Low], [Close], Volume
from #StockData t
where not exists (select 1
                  from StockPrice s 
				  where s.Symbol = '{this.TargetSymbol}'
				        and cast([TimeStamp] as date) = s.PriceDate
						and cast([TimeStamp] as time(0)) = s.PriceTime);"
                .Replace("<Table>", this.ApiFunction == ApiFunction.Daily ? "StockPriceDaily" : "StockPrice30Min");

                string script = $@"
begin try
    begin transaction;
    {insertScript}
    {updateScript}
    commit transaction;
end try
begin catch
    --do some stuff or whatever...
end catch
";
                
                SqlCommand cmd = new SqlCommand(insertScript, connection);
                
                cmd.ExecuteNonQuery();

                connection.Close();
            }

            Console.WriteLine("key agent {0} saved {1} to database", key, this.Descriptor);
        }
    }
}
