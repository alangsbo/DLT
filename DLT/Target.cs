using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace DLT
{
    public class Target
    {
        string connStr = "";
        string csvFolder = "";
        string csvSeparator = "";
        List<FetchTables> fetchTables;

        public Target(string TargetConnectionString, string CsvFolder, string CsvSeparator, List<FetchTables> FetchTables)
        {
            this.connStr = TargetConnectionString;
            this.csvFolder = CsvFolder;
            this.fetchTables = FetchTables;
            this.csvSeparator = CsvSeparator;
        }

        void BulkInsert(FetchTables ft, bool ParallelExecution, int MaxThreads, bool OracleSpool)
        {
            // If table is not incrementally loaded, all data is loaded to a temp table which is then switched
            // If incremental load, data is loaded into table 

            // create target schema if not exists
            TargetDataAccess.ExecSqlNonQuery("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '"+ft.TargetSchema+ "') BEGIN EXEC('CREATE SCHEMA " + ft.TargetSchema + "') END");

            // 1. Create temp table
            if (!ft.Incremental)
            {
                TargetDataAccess.ExecSqlNonQuery(ft.DropTempTableSql);
                TargetDataAccess.ExecSqlNonQuery(ft.CreateTempTableSql);
            }
            else
            {
                // if incremental, check if target table exists otherwise create it
                if (!CheckIfTableExists(ft))
                {
                    TargetDataAccess.ExecSqlNonQuery(ft.CreateTableSql);
                }
            }

            // 2. Bulk insert data to temp table
            Parallel.ForEach(ft.Shards, new ParallelOptions { MaxDegreeOfParallelism = MaxThreads/10 }, (shard) =>
            //foreach(Shard shard in ft.Shards)
            {
                Console.WriteLine($"Bulk inserting {shard.Name} on thread {Thread.CurrentThread.ManagedThreadId}");
                if(BulkInsert(shard, ft.Incremental, OracleSpool))
                {
                    FetchTables.NumShardsInsertedSuccessfully++;
                }
            }
            );

            if (!ft.Incremental)
            {
                // if all shards where inserted successfully
                if (ft.AllShardsBulkInsertedSuccessfully)
                {
                    // 3. Drop existsing table
                    TargetDataAccess.ExecSqlNonQuery(ft.DropTableSql);
                    // 4. Rename temp table
                    TargetDataAccess.ExecSqlNonQuery(ft.SwitchTableSql);
                }
            }
        }

        public void LoadTablesToTarget(bool paralellExection, int maxThreads)
        {
            LoadTablesToTarget(paralellExection, maxThreads, false);
        }

        public void LoadTablesToTarget(bool paralellExection, int maxThreads, bool OracleSpool)
        {
            if (paralellExection)
            {

                Parallel.ForEach(fetchTables, new ParallelOptions { MaxDegreeOfParallelism = maxThreads/10 }, (ft) =>
                {
                    Console.WriteLine($"Bulk inserting {ft.SourceTable} on thread {Thread.CurrentThread.ManagedThreadId}");
                    BulkInsert(ft, paralellExection, maxThreads, OracleSpool);
                });
            }
            else
            {
                foreach (FetchTables f in fetchTables)
                {
                    BulkInsert(f, paralellExection, maxThreads, OracleSpool);
                }
            }
        }
        

        bool BulkInsert(Shard shard, bool Incremental, bool OracleSpool)
        {
            string stepid = Guid.NewGuid().ToString();
            Logger.LogStepStart(stepid, shard.Name, "BULK INSERT " + shard.Name);
            string bulkinsertsql = "bulk insert " + shard.TargetSchema + "." + shard.TableName + (Incremental?" ": "_tmp ") +
                                    "from '" + csvFolder + shard.TableName + "\\" + shard.Name + ".csv' " +
                                    "with( " +
                                     "   format = 'csv', " +
                                     "   fieldterminator='" + csvSeparator + "'," +
                                     "   codepage = '"+ (OracleSpool ? "1252" : "65001") + "' " +
                                     (OracleSpool?"":",   firstrow = 2 ") +
                                     (OracleSpool ?"":",   fieldquote = '\"'")+
                                    ")";

            
            
            bool b = TargetDataAccess.ExecSqlNonQuery(bulkinsertsql);
            Logger.LogStepEnd(stepid);
            return b;

        }

        bool CheckIfTableExists(FetchTables ft)
        {

            string Sql = "select case when OBJECT_ID('" + ft.TargetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + "', 'U') is not null then 'true' else 'false' end";
            bool r = bool.Parse(TargetDataAccess.GetSingleValue(Sql).ToString());
            return r;
        }

        bool CheckIfTempTableExists(FetchTables ft)
        {

            string Sql = "select case when OBJECT_ID('" + ft.TargetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + "_tmp', 'U') is not null then 'true' else 'false' end";
            bool r = bool.Parse(TargetDataAccess.GetSingleValue(Sql).ToString());
            return r;
        }
    }
}
