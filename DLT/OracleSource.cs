using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using Oracle.DataAccess.Client;

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
            List<FetchTables> fetchTables = new List<FetchTables>();

            string[] lines = File.ReadAllLines("Config.txt");

            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i];
                if (line.Split(':')[0] == "fetchtable")
                {
                    string sourcechema = "", sourcetable = "", shardmethod = "", shardcolumn = "", incrementalcolumn = "", incrementalcolumntype = "";
                    bool loadtotarget = false, sharding = false, incremental = false;
                    int counter = 0;
                    for (int j = 1; j <= 9; j++)
                    {
                        if (i + j < lines.Length)
                        {

                            if (lines[i + j].Split(':')[0].Trim() == "sourceschema")
                            {
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
                            if (lines[i + j].Split(':')[0].Trim() == "incremental")
                            {
                                incremental = bool.Parse(lines[i + j].Split(':')[1].Trim());
                                counter++;
                            }
                            if (lines[i + j].Split(':')[0].Trim() == "incrementalcolumn")
                            {
                                incrementalcolumn = lines[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                            if (lines[i + j].Split(':')[0].Trim() == "incrementalcolumntype")
                            {
                                incrementalcolumntype = lines[i + j].Split(':')[1].Trim();
                                counter++;
                            }
                        }

                    }
                    i = i + counter;
                    FetchTables ft = new FetchTables(sourcechema, sourcetable, loadtotarget);
                    ft.Sharding = sharding;
                    ft.ShardMethod = shardmethod;
                    ft.ShardColumn = shardcolumn;
                    ft.Incremental = incremental;
                    ft.IncrementalColumn = incrementalcolumn;
                    ft.IncrementalColumnType = incrementalcolumntype;
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
    }
}
