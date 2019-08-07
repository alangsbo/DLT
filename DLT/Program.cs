using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;
using System.Data;

namespace DET
{
    public static class Log
    {
        public static long CsvBytesWritten = 0;
        public static DateTime CsvStartTime;
        public static DateTime CsvEndTime;
    }

    public class Shard
    {
        public string Sql;
        public string Name;
        public string TableName;
        public Shard(string Sql, string Name, string TableName)
        {
            this.Sql = Sql;
            this.Name = Name;
            this.TableName = TableName;
        }
    }

    class FetchTables
    {
        
        public string SourceSchema;
        public string SourceTable;
        public bool LoadToTarget =false;
        public bool Sharding = false;
        public string ShardMethod="";
        public string ShardColumn = "";

        public FetchTables(string SourceSchema, string SourceTable, bool LoadToTarget)
        {
            this.SourceSchema = SourceSchema;
            this.SourceTable = SourceTable;
            this.LoadToTarget = LoadToTarget;
        }

        public FetchTables(string SourceSchema, string SourceTable)
        {
            this.SourceSchema = SourceSchema;
            this.SourceTable = SourceTable;
        }

        public List<Shard> Shards
        {
            get
            { 
                // Return sql statement / statements (if sharding)
                List<Shard> shards = new List<Shard>();

                if (this.Sharding)
                {
                    switch (this.ShardMethod)
                    {
                        case "rightbase10":
                            for (int i = 0; i < 10; i++)
                            {
                                Shard s = new Shard("SELECT * FROM " + this.SourceSchema + "." + this.SourceTable + " WHERE RIGHT(CAST(" + this.ShardColumn + " as VARCHAR), 1) ='" + i.ToString() + "'", this.SourceSchema + "_" + this.SourceTable + "_" + i.ToString(), this.SourceSchema + "_" + this.SourceTable);
                                shards.Add(s);
                            }
                        break;
                    }
                }else
                {
                    shards.Add(new Shard("SELECT * FROM " + this.SourceSchema + "." + this.SourceTable, this.SourceSchema + "_" + this.SourceTable, this.SourceSchema + "_" + this.SourceTable));
                }

                return shards;
            }
        }
    }

    class Program
    {
        static string sourceConnStr = "";
        static string targetConnStr = "";
        static List<FetchTables> fetchTables = new List<FetchTables>();
        static string sqlServerMetadataFetchTemplate = "";
        static string targetSchema = "";
        static bool paralellExection = false;
        static int maxThreads = -1;

        static string csvFolder = "";

        static void Main(string[] args)
        {
            LoadConfig();

            Log.CsvStartTime = DateTime.Now;
            if (paralellExection)
            {
                List<Shard> allShards = new List<Shard>();
                foreach(FetchTables f in fetchTables)
                {
                    foreach(Shard s in f.Shards)
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
                foreach (FetchTables f in fetchTables)
                {
                    SaveTableAsCsv(f);
                }
            }
            Log.CsvEndTime = DateTime.Now;


            if (paralellExection)
            {
                
                Parallel.ForEach(fetchTables, new ParallelOptions { MaxDegreeOfParallelism = maxThreads }, (ft) =>
                {
                    Console.WriteLine($"Bulk inserting {ft.SourceTable} on thread {Thread.CurrentThread.ManagedThreadId}");
                    BulkInsert(ft);
                });
            }
            else
            {
                foreach (FetchTables f in fetchTables)
                {
                    BulkInsert(f); ;
                }
            }


            Console.WriteLine(Log.CsvBytesWritten / 1000000 + " MB loaded in " + (Log.CsvEndTime - Log.CsvStartTime).Seconds + " seconds - " + (Log.CsvBytesWritten / 1000000)/ (Log.CsvEndTime - Log.CsvStartTime).Seconds + " MB/s, " + ((Log.CsvBytesWritten / 1000000) / (Log.CsvEndTime - Log.CsvStartTime).Seconds)*8 + " MBPS");
            Console.WriteLine("Done...");
            Console.ReadLine();
        }

        static void LoadConfig()
        {
            sqlServerMetadataFetchTemplate = File.ReadAllText("SqlServerMetaDataFetchTemplate.txt");

            //IEnumerable<string> conns
            //StreamReader rdr = new StreamReader(("Config.txt"));
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
                if (line.Split(':')[0] == "paralellexecution")
                    paralellExection = bool.Parse(line.Split(':')[1].Trim());
                if (line.Split(':')[0] == "maxthreads")
                    maxThreads = int.Parse(line.Split(':')[1].Trim());
                if (line.Split(": ".ToCharArray())[0].ToString() == "csvfolder")
                {
                    csvFolder = line.Split(": ".ToCharArray(), 2)[1].ToString().Trim();
                    if(csvFolder.ToCharArray()[csvFolder.Length-1] != '\\')
                        csvFolder += "\\";
                }

                if (line.Split(':')[0] == "fetchtable")
                {
                    string sourcechema = "", sourcetable = "", shardmethod = "", shardcolumn = "";
                    bool loadtotarget = false, sharding = false;
                    int counter = 0;
                    for (int j = 1; j <=6; j++)
                    {
                        if(i+j < lines.Length)
                        { 
                        
                            if (lines[i+j].Split(':')[0].Trim() == "sourceschema") {
                                sourcechema = lines[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (lines[i + j].Split(':')[0].Trim() == "sourcetable")
                            {
                                sourcetable = lines[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (lines[i + j].Split(':')[0].Trim() == "loadtotarget")
                            {
                                loadtotarget = bool.Parse(lines[i + j].Split(':')[1].Trim());
                                counter++;
                            }
                            if (lines[i + j].Split(':')[0].Trim() == "sharding")
                            {
                                sharding = bool.Parse(lines[i + j].Split(':')[1].Trim());
                                counter++;
                            }
                            if (lines[i + j].Split(':')[0].Trim() == "shardmethod")
                            {
                                shardmethod = lines[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (lines[i + j].Split(':')[0].Trim() == "shardcolumn")
                            {
                                shardcolumn = lines[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                        }

                    }
                    i = i + counter;
                    FetchTables ft = new FetchTables(sourcechema, sourcetable, loadtotarget);
                    ft.Sharding = sharding;
                    ft.ShardMethod = shardmethod;
                    ft.ShardColumn = shardcolumn;
                    fetchTables.Add(ft);
                }
                
            }

        }

        static void ParallelMethod(string str)
        {

            int sleeptime = (new Random(DateTime.Now.Millisecond)).Next(1000, 10000);
            Console.WriteLine(str + " sleeping " + sleeptime + " seconds");
            Thread.Sleep(sleeptime);
            Console.WriteLine(str + " done");
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
                    if (o.GetType() == typeof(string))
                        val = "\"" + o.ToString().Replace("\"", "\"\"") + "\"";

                    else if (o.GetType() == typeof(bool))
                        val = (bool.Parse(o.ToString()) == false ? 0 : 1).ToString();

                    else if (o.GetType() == typeof(byte[]))
                        val = "";
                    else
                        val = o.ToString();

                    if (counter++ != output.Length - 1)
                        val += ",";

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


        static void BulkInsert(FetchTables ft)
        {
            // create temp table
            CreateTableFromSource(ft.SourceSchema, ft.SourceTable, true, true);
            Parallel.ForEach(ft.Shards, new ParallelOptions { MaxDegreeOfParallelism = maxThreads }, (shard) =>
            {
                Console.WriteLine($"Bulk inserting {shard.Name} on thread {Thread.CurrentThread.ManagedThreadId}");
                BulkInsert(shard);
            });

            SwitchTable(ft);


        }

        static void SwitchTable(FetchTables ft)
        {
            try
            {
                // ExecSqlNonQuery("EXEC sp_rename '" + targetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + "', '" + ft.SourceSchema + "_" + ft.SourceTable + "_old';", targetConnStr);
                string deleteSql = "IF OBJECT_ID('" + targetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + "', 'U') IS NOT NULL   DROP TABLE " + targetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + ";";
                ExecSqlNonQuery(deleteSql, targetConnStr);
            }
            catch (Exception ex)
            { }
            ExecSqlNonQuery("EXEC sp_rename '"+ targetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + "_tmp', '" + ft.SourceSchema + "_" + ft.SourceTable + "';", targetConnStr);
        }

        static void BulkInsert(Shard shard)
        {

            //string truncatesql = "TRUNCATE TABLE " + targetSchema + "." + sourceSchema + "_" + sourceTable;

            //ExecSqlNonQuery(truncatesql, targetConnStr);

            string bulkinsertsql = "bulk insert " + targetSchema + "." + shard.TableName + "_tmp " +
                                    "from '"+csvFolder+shard.Name + ".csv' " +
                                    "with( " +
                                     "   format = 'csv', " +
                                     "   firstrow = 2, " +
                                     "   fieldquote = '\"', " +
                                     "   codepage = '65001' " +
                                    ")";
            ExecSqlNonQuery(bulkinsertsql, targetConnStr);
            
        }

        static DataSet GetTableMetaData(string schema, string TableName)
        {
            string fetchMetadataSql = sqlServerMetadataFetchTemplate.Replace("%SCHEMANAME%", schema).Replace("%TABLENAME%", TableName);
            DataSet ds = GetDataSet(sourceConnStr, fetchMetadataSql);
            return ds;
        }

        static string GetCreateTableSql(string schema, string TableName, bool TempTable)
        {
            DataSet ds = GetTableMetaData(schema, TableName);

            string sql = "CREATE TABLE " + targetSchema + "." + schema + "_" + TableName + (TempTable?"_tmp":"") + " ( " + Environment.NewLine;

            int counter = 0;
            foreach(DataRow r in ds.Tables[0].Rows)
            {
                if (counter++ == 0)
                    sql += "    ";
                else
                    sql += "   ,";

                sql += r["col_name"].ToString() + " " + r["datatype"].ToString()  + Environment.NewLine;
            }

            sql += ")" + Environment.NewLine;

            return sql;
        }

        static void CreateTableFromSource(string sourceSchema, string sourceTable, bool replaceTableIfExists, bool tempTable)
        {
            if(!tempTable)
            {
                if (replaceTableIfExists)
                {
                    string deleteSql = "IF OBJECT_ID('" + targetSchema + "." + sourceSchema + "_" + sourceTable + "', 'U') IS NOT NULL   DROP TABLE " + targetSchema + "." + sourceSchema + "_" + sourceTable + ";";
                    ExecSqlNonQuery(deleteSql, targetConnStr);
                }
                Console.WriteLine("Creating table " + targetSchema + "." + sourceSchema + "_" + sourceTable);

                string sql = GetCreateTableSql(sourceSchema, sourceTable, false);
                Console.WriteLine(sql);
                ExecSqlNonQuery(sql, targetConnStr);
            }
            else
            {
                string deleteSql = "IF OBJECT_ID('" + targetSchema + "." + sourceSchema + "_" + sourceTable + "_tmp', 'U') IS NOT NULL   DROP TABLE " + targetSchema + "." + sourceSchema + "_" + sourceTable + ";";
                ExecSqlNonQuery(deleteSql, targetConnStr);
                Console.WriteLine("Creating temp table " + targetSchema + "." + sourceSchema + "_" + sourceTable+"_tmp");

                string sql = GetCreateTableSql(sourceSchema, sourceTable, true);
                Console.WriteLine(sql);
                ExecSqlNonQuery(sql, targetConnStr);
            }
            
        }

        static public DataSet GetDataSet(string ConnectionString, string SQL)
        {
            SqlConnection conn = new SqlConnection(ConnectionString);
            SqlDataAdapter da = new SqlDataAdapter();
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = SQL;
            da.SelectCommand = cmd;
            DataSet ds = new DataSet();
            try
            {
                conn.Open();
                da.Fill(ds);
                conn.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return ds;
        }

        static void ExecSqlNonQuery(string sql, string connStr)
        {
            SqlConnection conn = new SqlConnection(connStr);
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            try
            {
                conn.Open();
                cmd.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(sql);
            }
        }

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
