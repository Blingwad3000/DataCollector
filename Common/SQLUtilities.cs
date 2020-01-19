using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace DataCollector
{
    public static class SQLUtilities
    {
        public static void ExecuteScript(string script)
        {
            using (SqlConnection connection = new SqlConnection(AlphaVantageConfigs.ConnectionString))
            {
                connection.Open();

                SqlCommand cmd = new SqlCommand(script, connection);

                cmd.ExecuteNonQuery();

                connection.Close();
            }
        }

        public static void BulkWrite<T>(List<T> data, string tableName, SqlConnection connection)
        {
            connection.Open();

            BulkWrite<T>(data, connection, tableName);

            connection.Close();
        }

        public static void BulkWrite<T>(List<T> data, SqlConnection connection, string tableName, bool dropExisting = true)
        {
            DataTable table = ToDataTable(data);

            SQLCreateTable<T>(connection, tableName, dropExisting);

            SqlBulkCopy blk = new SqlBulkCopy(connection);
            blk.DestinationTableName = tableName;

            blk.WriteToServer(table);
        }

        public static DataTable ToDataTable<T>(List<T> data)
        {
            DataTable result = new DataTable();

            var properties = typeof(T).GetFields().Where(p => ConvertDotNetToSQLType(p.FieldType) != "");

            foreach (FieldInfo p in properties)
            {
                DataColumn c = result.Columns.Add(p.Name, p.FieldType);
            }

            foreach (T element in data)
            {
                DataRow row = result.NewRow();

                foreach (FieldInfo p in properties)
                    row[p.Name] = p.GetValue(element);

                result.Rows.Add(row);
            }

            return result;
        }

        public static List<T> FromDataTable<T>(DataTable table)
        {
            var columnNames = table.Columns.Cast<DataColumn>()
    .Select(c => c.ColumnName)
    .ToList();

            var fields = typeof(T).GetFields();

            return table.AsEnumerable().Select(row =>
            {
                var objT = Activator.CreateInstance<T>();

                foreach (var pro in fields)
                {
                    if (columnNames.Contains(pro.Name))
                        pro.SetValue(objT, row[pro.Name].GetType() == typeof(System.DBNull) ? 0 : row[pro.Name]);
                }

                return objT;
            }).ToList();
        }

        public static List<T> FromQuery<T>(string query)
        {
            DataTable table = new DataTable();

            using (SqlConnection connection = new SqlConnection(AlphaVantageConfigs.ConnectionString))
            {
                connection.Open();

                SqlCommand cmd = new SqlCommand(query,
                                                connection);

                SqlDataAdapter da = new SqlDataAdapter(cmd);
                
                da.Fill(table);
                
                connection.Close();
            }

            return FromDataTable<T>(table);
        }

        public static void SQLCreateTable<T>(SqlConnection connection, string tableName, bool dropExisting = true)
        {
            var properties = typeof(T).GetFields();

            string createTableString = "drop table if exists " + tableName +
                                       "\ncreate table " + tableName + "(" + string.Join(", ", properties.Where(p => ConvertDotNetToSQLType(p.FieldType) != "")
                                                                                 .Select(p => p.Name + " " + ConvertDotNetToSQLType(p.FieldType))) + ");";

            if (!tableName.StartsWith("#") && !tableName.StartsWith("admin.dbo."))
            {
                throw new Exception("SQLCreateTable is only intended for use in the ADMIN database or with temp tables");
            }

            if (!tableName.StartsWith("#"))
            {
                string[] parts = tableName.Split('.');

                createTableString = "if not exists (select 1 from admin.information_schema.tables where table_name='" + parts[2] + "')\nbegin\n" + createTableString + "\nend";
            }

            SqlCommand cmd = new SqlCommand(createTableString, connection);
            cmd.CommandType = CommandType.Text;

            cmd.ExecuteNonQuery();
        }

        public static string ConvertDotNetToSQLType(Type t)
        {
            if (t == typeof(string))   return "varchar(4000)";
            if (t == typeof(char[]))   return "varchar(4000)";
            if (t == typeof(int))      return "int";
            if (t == typeof(Int32))    return "int";
            if (t == typeof(Int16))    return "smallint";
            if (t == typeof(Int64))    return "bigint";
            if (t == typeof(Byte[]))   return "varbinary(4000)";
            if (t == typeof(Boolean))  return "bit";
            if (t == typeof(DateTime)) return "DateTime2(3)";
            if (t == typeof(Decimal))  return "decimal(19,6)";
            if (t == typeof(Double))   return "float";
            if (t == typeof(Byte))     return "tinyint";
            if (t == typeof(TimeSpan)) return "time(0)";

            return "";
        }
    }
}
