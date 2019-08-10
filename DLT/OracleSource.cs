using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using Oracle.DataAccess.Client;
using System.Threading;

namespace DLT
{
    class OracleSource
    {
        string oracleSourceMetadataFetchTemplate = "";
        string oracleSourceConnectionString = "";

        public OracleSource(string SqlServerSourceConnectionString)
        {
            oracleSourceMetadataFetchTemplate = File.ReadAllText("OracleMetadataFetchTemplate.txt");
            oracleSourceConnectionString = SqlServerSourceConnectionString;
        }

        public List<FetchTables> LoadTablesFromConfig()
        {
            return LoadTablesFromConfig(-1);
        }

        public List<FetchTables> LoadTablesFromConfig(int MaxRowLimit)
        {
            List<FetchTables> fetchTables = new List<FetchTables>();

            string[] lines = File.ReadAllLines("Config.txt");
            List<string> linesWithoutComments = new List<string>();

            foreach (string line in lines)
            {
                if (line.Length > 2 && line.Substring(0, 2) != "//" && line.Substring(0, 2) != "--")
                {
                    linesWithoutComments.Add(line);
                }
            }

            for (int i = 0; i < linesWithoutComments.Count; i++)
            {
                string line = linesWithoutComments[i];
                if (line.Split(':')[0] == "fetchtable")
                {
                    string sourcechema = "", sourcetable = "", shardmethod = "", shardcolumn = "", incrementalcolumn = "", incrementalcolumntype = "", where="";
                    bool loadtotarget = false, sharding = false, incremental = false;
                    int counter = 0;
                    for (int j = 1; j <= 9; j++)
                    {
                        if (i + j < linesWithoutComments.Count)
                        {

                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "sourceschema")
                            {
                                sourcechema = linesWithoutComments[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "sourcetable")
                            {
                                sourcetable = linesWithoutComments[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "loadtotarget")
                            {
                                loadtotarget = bool.Parse(linesWithoutComments[i + j].Split(':')[1].Trim());
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "sharding")
                            {
                                sharding = bool.Parse(linesWithoutComments[i + j].Split(':')[1].Trim());
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "shardmethod")
                            {
                                shardmethod = linesWithoutComments[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "shardcolumn")
                            {
                                shardcolumn = linesWithoutComments[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "incremental")
                            {
                                incremental = bool.Parse(linesWithoutComments[i + j].Split(':')[1].Trim());
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "incrementalcolumn")
                            {
                                incrementalcolumn = linesWithoutComments[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "incrementalcolumntype")
                            {
                                incrementalcolumntype = linesWithoutComments[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "where")
                            {
                                where = linesWithoutComments[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (linesWithoutComments[i + j].Split(':')[0].Trim() == "fetchtable")
                            {
                                break;
                            }
                        }

                    }
                    i = i + counter;
                    FetchTables ft = new FetchTables(sourcechema, sourcetable, loadtotarget, "Oracle");
                    ft.Sharding = sharding;
                    ft.ShardMethod = shardmethod;
                    ft.ShardColumn = shardcolumn;
                    ft.Incremental = incremental;
                    ft.IncrementalColumn = incrementalcolumn;
                    ft.IncrementalColumnType = incrementalcolumntype;
                    ft.LimitRowsForTest = MaxRowLimit;
                    ft.Where = where;
                    fetchTables.Add(ft);
                }
            }

            // Populate each fetchtable with table creation sql´s by fetching metadata from data source
            foreach (FetchTables fetchTable in fetchTables)
            {
                fetchTable.CreateTableSql = GetCreateTableSql(fetchTable.SourceSchema, fetchTable.SourceTable, false);
                fetchTable.CreateTempTableSql = GetCreateTableSql(fetchTable.SourceSchema, fetchTable.SourceTable, true);
                fetchTable.DropTableSql = "IF OBJECT_ID('" + "dlt" + "." + fetchTable.SourceSchema + "_" + fetchTable.SourceTable + "', 'U') IS NOT NULL   DROP TABLE " + "dlt" + "." + fetchTable.SourceSchema + "_" + fetchTable.SourceTable + ";";
                fetchTable.SwitchTableSql = "EXEC sp_rename '" + "dlt"+ "." + fetchTable.SourceSchema + "_" + fetchTable.SourceTable + "_tmp', '" + fetchTable.SourceSchema + "_" + fetchTable.SourceTable + "';";
            }

           

            return fetchTables;
        }

        public string GetCreateTableSql(string schema, string TableName, bool TempTable)
        {
            DataSet ds = GetTableMetaData(schema, TableName);

            string sql = "CREATE TABLE " + "dlt" + "." + schema + "_" + TableName + (TempTable ? "_tmp" : "") + " ( " + Environment.NewLine;

            int counter = 0;
            foreach (DataRow r in ds.Tables[0].Rows)
            {
                if (counter++ == 0)
                    sql += "    ";
                else
                    sql += "   ,";

                sql += r["col_name"].ToString() + " " + r["datatype"].ToString() + Environment.NewLine;
            }

            sql += ")" + Environment.NewLine;

            return sql;
        }

        public DataSet GetTableMetaData(string schema, string TableName)
        {
            string fetchMetadataSql = this.oracleSourceMetadataFetchTemplate.Replace("%SCHEMANAME%", schema).Replace("%TABLENAME%", TableName);
            DataSet ds = GetDataSet(fetchMetadataSql);
            return ds;
        }

        public DataSet GetDataSet(string SQL)
        {
            OracleConnection conn = new OracleConnection(this.oracleSourceConnectionString);
            OracleDataAdapter da = new OracleDataAdapter();
            OracleCommand cmd = conn.CreateCommand();
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

        public void ExportTablesAsCsv(List<FetchTables> ft, bool parallelExecution, int maxThreads, string CsvFolder, string csvSeparator)
        {
            if (parallelExecution)
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
                    SaveShardAsCsv(shard, CsvFolder, csvSeparator);
                });
            }
            else
            {
                foreach (FetchTables f in ft)
                {
                    SaveTableAsCsv(f, CsvFolder, csvSeparator);
                }
            }
        }

        public void SaveShardAsCsv(Shard shard, string csvFolder, string csvSeparator)
        {
            SaveShardAsCsv(shard, csvFolder, csvSeparator, "Unicode");
        }

        public void SaveShardAsCsv(Shard shard, string csvFolder, string csvSeparator, string encoding)
        {
            Console.WriteLine($"Downloading {shard.Name} on thread {Thread.CurrentThread.ManagedThreadId}");

            OracleConnection oraCon = new OracleConnection(this.oracleSourceConnectionString);
            oraCon.Open();

            OracleCommand cmd = new OracleCommand(shard.Sql, oraCon);
            OracleDataReader reader = cmd.ExecuteReader();

            string fileName = csvFolder + shard.Name + ".csv";
            StreamWriter sw = null;
            if (encoding == "UTF8")
            {
                sw = new StreamWriter(fileName, false, Encoding.UTF8);
            }
            else
            {
                sw = new StreamWriter(fileName, false, Encoding.Unicode);
            }
            
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
                        val = "\"" + (bool.Parse(o.ToString()) == false ? 0 : 1).ToString() + "\"";

                    else if (reader.GetDataTypeName(counter) == "Double")
                        val = "\"" + o.ToString().Replace(",", ".") + "\"";

                    else if (reader.GetDataTypeName(counter) == "Decimal")
                        val = "\"" + o.ToString().Replace(",", ".") + "\"";

                    else if (reader.GetDataTypeName(counter) == "Numeric")
                        val = "\"" + o.ToString().Replace(",", ".") + "\"";
                                        
                    else if (o.GetType() == typeof(byte[]))
                        val = "\"\"";
                    else
                        val = o.ToString().Replace(",", ".").Replace("\"", "");

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
            oraCon.Close();
        }

        public void SaveTableAsCsv(FetchTables TableToFetch, string CsvFolder, string csvSeparator)
        {
            foreach (Shard shard in TableToFetch.Shards)
            {
                SaveShardAsCsv(shard, CsvFolder, csvSeparator);
            }
        }
    }
}
