using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;
using System.Data;

namespace DLT
{
    public static class Log
    {
        public static long CsvBytesWritten = 0;
        public static DateTime CsvStartTime;
        public static DateTime CsvEndTime;
    }

    

    

    public class Program
    {
        static string sourceConnStr = "";
        static string targetConnStr = "";
        static string csvSeparator = "";
       // static List<FetchTables> fetchTables = new List<FetchTables>();
        static bool skipCsv = false;
        //static string sqlServerMetadataFetchTemplate = "";
        static string targetSchema = "";
        static bool paralellExection = false;
        static int maxThreads = -1;

        static string csvFolder = "";

        static void Main(string[] args)
        {

            LoadConfig();
            List<FetchTables> ft = (new SqlServerSource(sourceConnStr)).LoadTablesFromConfig();

     
            //GetCreateTableSql(fetchTables);

            if (!skipCsv)
            {
                Log.CsvStartTime = DateTime.Now;
                if (paralellExection)
                {
                    List<Shard> allShards = new List<Shard>();
                    foreach (FetchTables f in ft)
                    {
                        foreach (Shard s in f.Shards)
                        {
                            allShards.Add(s);
                        }
                    }

                    Parallel.ForEach(allShards, new ParallelOptions { MaxDegreeOfParallelism = maxThreads }, (shard) =>
                    {
                        SaveCsv(shard, sourceConnStr);
                    });
                }
                else
                {
                    foreach (FetchTables f in ft)
                    {
                        SaveTableAsCsv(f);
                    }
                }
                Log.CsvEndTime = DateTime.Now;
                try
                {
                    Console.WriteLine("Csv Load started: " + Log.CsvStartTime.ToLongTimeString());
                    Console.WriteLine("Csv Load started: " + Log.CsvEndTime.ToLongTimeString());
                    Console.WriteLine(Log.CsvBytesWritten / 1000000 + " MB loaded in " + (Log.CsvEndTime - Log.CsvStartTime).Seconds + " seconds - " + (Log.CsvBytesWritten / 1000000) / (Log.CsvEndTime - Log.CsvStartTime).Seconds + " MB/s, " + ((Log.CsvBytesWritten / 1000000) / (Log.CsvEndTime - Log.CsvStartTime).TotalSeconds) * 8 + " MBPS");
                }
                catch (Exception ex) { }
            }

           Target t = new Target(targetConnStr, targetSchema, csvFolder, csvSeparator, ft);
            t.LoadTablesToTarget(paralellExection, maxThreads);

            Console.WriteLine("Done...");
            Console.ReadLine();
        }

        static void LoadConfig()
        {
            //sqlServerMetadataFetchTemplate = File.ReadAllText("SqlServerMetaDataFetchTemplate.txt");
;
            string[] lines = File.ReadAllLines("Config.txt");

            for(int i=0;i<lines.Length;i++)
            {
                string line = lines[i];
                if (line.Split(':')[0] == "source")
                    sourceConnStr = line.Split(':')[1].Trim();
                if (line.Split(':')[0] == "dest")
                    targetConnStr = line.Split(':')[1].Trim();
                if (line.Split(':')[0] == "targetschema")
                    targetSchema = line.Split(':')[1].Trim();
                if (line.Split(':')[0] == "csvseparator")
                    csvSeparator = line.Split(':')[1].Trim();
                if (line.Split(':')[0] == "paralellexecution")
                    paralellExection = bool.Parse(line.Split(':')[1].Trim());
                if (line.Split(':')[0] == "skipcsv")
                    skipCsv = bool.Parse(line.Split(':')[1].Trim());
                if (line.Split(':')[0] == "maxthreads")
                    maxThreads = int.Parse(line.Split(':')[1].Trim());
                if (line.Split(": ".ToCharArray())[0].ToString() == "csvfolder")
                {
                    csvFolder = line.Split(": ".ToCharArray(), 2)[1].ToString().Trim();
                    if(csvFolder.ToCharArray()[csvFolder.Length-1] != '\\')
                        csvFolder += "\\";
                }

                //if (line.Split(':')[0] == "fetchtable")
                //{
                //    string sourcechema = "", sourcetable = "", shardmethod = "", shardcolumn = "", incrementalcolumn="", incrementalcolumntype="";
                //    bool loadtotarget = false, sharding = false, incremental = false;
                //    int counter = 0;
                //    for (int j = 1; j <=9; j++)
                //    {
                //        if(i+j < lines.Length)
                //        { 
                        
                //            if (lines[i+j].Split(':')[0].Trim() == "sourceschema") {
                //                sourcechema = lines[i + j].Split(':')[1].Trim();
                //                counter++;
                //            }
                //            if (lines[i + j].Split(':')[0].Trim() == "sourcetable")
                //            {
                //                sourcetable = lines[i + j].Split(':')[1].Trim();
                //                counter++;
                //            }
                //            if (lines[i + j].Split(':')[0].Trim() == "loadtotarget")
                //            {
                //                loadtotarget = bool.Parse(lines[i + j].Split(':')[1].Trim());
                //                counter++;
                //            }
                //            if (lines[i + j].Split(':')[0].Trim() == "sharding")
                //            {
                //                sharding = bool.Parse(lines[i + j].Split(':')[1].Trim());
                //                counter++;
                //            }
                //            if (lines[i + j].Split(':')[0].Trim() == "shardmethod")
                //            {
                //                shardmethod = lines[i + j].Split(':')[1].Trim();
                //                counter++;
                //            }
                //            if (lines[i + j].Split(':')[0].Trim() == "shardcolumn")
                //            {
                //                shardcolumn = lines[i + j].Split(':')[1].Trim();
                //                counter++;
                //            }
                //            if (lines[i + j].Split(':')[0].Trim() == "incremental")
                //            {
                //                incremental = bool.Parse(lines[i + j].Split(':')[1].Trim());
                //                counter++;
                //            }
                //            if (lines[i + j].Split(':')[0].Trim() == "incrementalcolumn")
                //            {
                //                incrementalcolumn = lines[i + j].Split(':')[1].Trim();
                //                counter++;
                //            }
                //            if (lines[i + j].Split(':')[0].Trim() == "incrementalcolumntype")
                //            {
                //                incrementalcolumntype = lines[i + j].Split(':')[1].Trim();
                //                counter++;
                //            }
                //        }

                //    }
                    //i = i + counter;
                    //FetchTables ft = new FetchTables(sourcechema, sourcetable, loadtotarget);
                    //ft.Sharding = sharding;
                    //ft.ShardMethod = shardmethod;
                    //ft.ShardColumn = shardcolumn;
                    //ft.Incremental = incremental;
                    //ft.IncrementalColumn = incrementalcolumn;
                    //ft.IncrementalColumnType = incrementalcolumntype;
                    //fetchTables.Add(ft);
                //}
                
            }

            // Initialie Target Data Acccess
            TargetDataAccess.TargetConnStr = targetConnStr;

        }

        static void SaveTableAsCsv(FetchTables TableToFetch)
        {
            foreach(Shard shard in TableToFetch.Shards)
            {
                SaveCsv(shard, sourceConnStr);
            }
        }

        static void SaveCsv(Shard shard, string connStr)
        {
            Console.WriteLine($"Downloading {shard.Name} on thread {Thread.CurrentThread.ManagedThreadId}");

            SqlConnection sqlCon = new SqlConnection(connStr);
            sqlCon.Open();

            SqlCommand sqlCmd = new SqlCommand(shard.Sql, sqlCon);
            SqlDataReader reader = sqlCmd.ExecuteReader();

            string fileName = csvFolder + shard.Name + ".csv";
            StreamWriter sw = new StreamWriter(fileName, false, Encoding.Unicode);
            object[] output = new object[reader.FieldCount];

            for (int i = 0; i < reader.FieldCount; i++)
                output[i] = reader.GetName(i);

            sw.WriteLine(string.Join(",", output));

            while (reader.Read())
            {
                reader.GetValues(output);
                string row = "";
                int counter = 0;
                foreach (object o in output)
                {
                    string val = "";
                    if (reader.GetDataTypeName(counter) == "varchar" || reader.GetDataTypeName(counter) == "nvarchar")
                        val = "\"" + o.ToString().Replace("\"", "\"\"") + "\"";

                    else if (o.GetType() == typeof(bool))
                        val = "\"" +  (bool.Parse(o.ToString()) == false ? 0 : 1).ToString() + "\"";

                    else if (reader.GetDataTypeName(counter) == "decimal")
                        val = "\"" + o.ToString().Replace(",", ".") + "\"";

                    else if (o.GetType() == typeof(byte[]))
                        val = "\"\"";
                    else
                        val = "\"" + o.ToString() + "\"";

                    if (counter++ != output.Length - 1)
                        val += csvSeparator;

                    row += val;
                }
                sw.WriteLine(row);  
            }

            sw.Flush();
            Log.CsvBytesWritten += sw.BaseStream.Length;
            sw.Close();
            reader.Close();
            sqlCon.Close();
        }

        //static void GetCreateTableSql(List<FetchTables> ft)
        //{
        //    foreach(FetchTables fetchTable in ft)
        //    {
        //        fetchTable.CreateTableSql = GetCreateTableSql(fetchTable.SourceSchema, fetchTable.SourceTable, false);
        //        fetchTable.CreateTempTableSql = GetCreateTableSql(fetchTable.SourceSchema, fetchTable.SourceTable, true);
        //        fetchTable.DropTableSql = "IF OBJECT_ID('" + targetSchema + "." + fetchTable.SourceSchema + "_" + fetchTable.SourceTable + "', 'U') IS NOT NULL   DROP TABLE " + targetSchema + "." + fetchTable.SourceSchema + "_" + fetchTable.SourceTable + ";";
        //        fetchTable.SwitchTableSql = "EXEC sp_rename '" + targetSchema + "." + fetchTable.SourceSchema + "_" + fetchTable.SourceTable + "_tmp', '" + fetchTable.SourceSchema + "_" + fetchTable.SourceTable + "';";
        //    }
        //}
        
        //static DataSet GetTableMetaData(string schema, string TableName)
        //{
        //    string fetchMetadataSql = null;// sqlServerMetadataFetchTemplate.Replace("%SCHEMANAME%", schema).Replace("%TABLENAME%", TableName);
        //    DataSet ds = GetDataSet(sourceConnStr, fetchMetadataSql);
        //    return ds;
        //}

        //static string GetCreateTableSql(string schema, string TableName, bool TempTable)
        //{
        //    DataSet ds = GetTableMetaData(schema, TableName);

        //    string sql = "CREATE TABLE " + targetSchema + "." + schema + "_" + TableName + (TempTable?"_tmp":"") + " ( " + Environment.NewLine;

        //    int counter = 0;
        //    foreach(DataRow r in ds.Tables[0].Rows)
        //    {
        //        if (counter++ == 0)
        //            sql += "    ";
        //        else
        //            sql += "   ,";

        //        sql += r["col_name"].ToString() + " " + r["datatype"].ToString()  + Environment.NewLine;
        //    }

        //    sql += ")" + Environment.NewLine;

        //    return sql;
        //}

        //static public DataSet GetDataSet(string ConnectionString, string SQL)
        //{
        //    SqlConnection conn = new SqlConnection(ConnectionString);
        //    SqlDataAdapter da = new SqlDataAdapter();
        //    SqlCommand cmd = conn.CreateCommand();
        //    cmd.CommandText = SQL;
        //    da.SelectCommand = cmd;
        //    DataSet ds = new DataSet();
        //    try
        //    {
        //        conn.Open();
        //        da.Fill(ds);
        //        conn.Close();
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.Message);
        //    }

        //    return ds;
        //}
       
        static string ConvertToCSV(DataSet objDataSet)
        {
            StringBuilder content = new StringBuilder();

            if (objDataSet.Tables.Count >= 1)
            {
                DataTable table = objDataSet.Tables[0];

                if (table.Rows.Count > 0)
                {
                    DataRow dr1 = (DataRow)table.Rows[0];
                    int intColumnCount = dr1.Table.Columns.Count;
                    int index = 1;

                    //add column names
                    foreach (DataColumn item in dr1.Table.Columns)
                    {
                        content.Append(String.Format("\"{0}\"", item.ColumnName));
                        if (index < intColumnCount)
                            content.Append(",");
                        else
                            content.Append("\r\n");
                        index++;
                    }

                    //add column data
                    foreach (DataRow currentRow in table.Rows)
                    {
                        string strRow = string.Empty;
                        for (int y = 0; y <= intColumnCount - 1; y++)
                        {
                            strRow += "\"" + currentRow[y].ToString() + "\"";

                            if (y < intColumnCount - 1 && y >= 0)
                                strRow += ",";
                        }
                        content.Append(strRow + "\r\n");
                    }
                }
            }

            return content.ToString();
        }

        
    }
}
