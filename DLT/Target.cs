﻿using System;
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
        string targetSchema = "";
        string csvFolder = "";
        string csvSeparator = "";
        List<FetchTables> fetchTables;

        public Target(string TargetConnectionString, string TargetSchema, string CsvFolder, string CsvSeparator, List<FetchTables> FetchTables)
        {
            this.connStr = TargetConnectionString;
            this.targetSchema = TargetSchema;
            this.csvFolder = CsvFolder;
            this.fetchTables = FetchTables;
            this.csvSeparator = CsvSeparator;
        }

        void BulkInsert(FetchTables ft, bool ParallelExecution, int MaxThreads, bool OracleSpool)
        {
            // If table is not incrementally loaded, all data is loaded to a temp table which is then switched
            // If incremental load, data is loaded into table 

            // 1. Create temp table
            if (!ft.Incremental) { 
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

        //void SwitchTable(FetchTables ft)
        //{
        //    try
        //    {
        //        // DataAccess.ExecSqlNonQuery("EXEC sp_rename '" + targetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + "', '" + ft.SourceSchema + "_" + ft.SourceTable + "_old';", targetConnStr);
        //        string deleteSql = "IF OBJECT_ID('" + targetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + "', 'U') IS NOT NULL   DROP TABLE " + targetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + ";";
        //        DataAccess.ExecSqlNonQuery(deleteSql, connStr);
        //    }
        //    catch (Exception ex)
        //    { }
        //    DataAccess.ExecSqlNonQuery("EXEC sp_rename '" + targetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + "_tmp', '" + ft.SourceSchema + "_" + ft.SourceTable + "';", connStr);
        //}

        bool BulkInsert(Shard shard, bool Incremental, bool OracleSpool)
        {

            //string truncatesql = "TRUNCATE TABLE " + targetSchema + "." + sourceSchema + "_" + sourceTable;

            //DataAccess.ExecSqlNonQuery(truncatesql, targetConnStr);

            string bulkinsertsql = "bulk insert " + targetSchema + "." + shard.TableName + (Incremental?" ": "_tmp ") +
                                    "from '" + csvFolder + shard.TableName + "\\" + shard.Name + ".csv' " +
                                    "with( " +
                                     "   format = 'csv', " +
                                     "   fieldterminator='" + csvSeparator + "'," +
                                     "   codepage = '"+ (OracleSpool ? "1252" : "65001") + "' " +
                                     (OracleSpool?"":",   firstrow = 2, ") +
                                     (OracleSpool ?"":",   fieldquote = '\"'")+
                                    ")";
            return TargetDataAccess.ExecSqlNonQuery(bulkinsertsql);

        }

        bool CheckIfTableExists(FetchTables ft)
        {

            string Sql = "select case when OBJECT_ID('" + targetSchema + "." + ft.SourceSchema + "_" + ft.SourceTable + "', 'U') is not null then 'true' else 'false' end";
            bool r = bool.Parse(TargetDataAccess.GetSingleValue(Sql).ToString());
            return r;
        }

        void CreateTableFromSource(string sourceSchema, string sourceTable, bool replaceTableIfExists, bool tempTable)
        {
            //if (!tempTable)
            //{
            //    if (replaceTableIfExists)
            //    {
            //        string deleteSql = "IF OBJECT_ID('" + targetSchema + "." + sourceSchema + "_" + sourceTable + "', 'U') IS NOT NULL   DROP TABLE " + targetSchema + "." + sourceSchema + "_" + sourceTable + ";";
            //        DataAccess.ExecSqlNonQuery(deleteSql, connStr);
            //    }
            //    Console.WriteLine("Creating table " + targetSchema + "." + sourceSchema + "_" + sourceTable);

            //    string sql = GetCreateTableSql(sourceSchema, sourceTable, false);
            //    Console.WriteLine(sql);
            //    DataAccess.ExecSqlNonQuery(sql, connStr);
            //}
            //else
            //{
            //    string deleteSql = "IF OBJECT_ID('" + targetSchema + "." + sourceSchema + "_" + sourceTable + "_tmp', 'U') IS NOT NULL   DROP TABLE " + targetSchema + "." + sourceSchema + "_" + sourceTable + ";";
            //    DataAccess.ExecSqlNonQuery(deleteSql, connStr);
            //    Console.WriteLine("Creating temp table " + targetSchema + "." + sourceSchema + "_" + sourceTable + "_tmp");

            //    string sql = GetCreateTableSql(sourceSchema, sourceTable, true);
            //    Console.WriteLine(sql);
            //    DataAccess.ExecSqlNonQuery(sql, connStr);
            //}

        }

       
    

    }
}
