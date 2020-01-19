using System;
using System.Data.SqlClient;

namespace DataCollector
{
    public class StockDataPoint
    {
        public DateTime PriceDate;
        public double   Open;
        public double   High;
        public double   Low;
        public double   Close;
        public double   Volume;

        public StockDataPoint(string[] dataRow)
        {
            this.PriceDate = DateTime.Parse(dataRow[0]);
            this.Open = double.Parse(dataRow[1]);
            this.High = double.Parse(dataRow[2]);
            this.Low = double.Parse(dataRow[3]);
            this.Close = double.Parse(dataRow[4]);
            this.Volume = double.Parse(dataRow[5]);
        }

        public static void SqlCreateTempTable(SqlConnection connection, string tableName = "#StockData")
        {
            string script = @"drop table if exists #StockData;
create table <TableName> ([PriceDate] datetime not null, [Open] float not null, High float not null, Low float not null, [Close] float not null, Volume float not null);"
.Replace("<TableName>", "#StockData");

            SqlCommand cmd = new SqlCommand(script, connection);
            cmd.ExecuteNonQuery();
        }
    }
}
