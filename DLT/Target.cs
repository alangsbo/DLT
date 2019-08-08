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

        void BulkInsert(FetchTables ft, bool ParallelExecution, int MaxThreads)
        {
            // If table is not incrementally loaded, all data is loaded to a temp table which is then switched
            // If incremental load, data is loaded into table 

            // 1. Create temp table
            if(!ft.Incremental)
                TargetDataAccess.ExecSqlNonQuery(ft.CreateTempTableSql);

            // 2. Bulk insert data to temp table
            Parallel.ForEach(ft.Shards, new ParallelOptions { MaxDegreeOfParallelism = MaxThreads/10 }, (shard) =>
            {
                Console.WriteLine($"Bulk inserting {shard.Name} on thread {Thread.CurrentThread.ManagedThreadId}");
                BulkInsert(shard, ft.Incremental);
            });

           if (!ft.Incremental) {
                // 3. Drop existsing table
                TargetDataAccess.ExecSqlNonQuery(ft.DropTableSql);
                // 4. Rename temp table
                TargetDataAccess.ExecSqlNonQuery(ft.SwitchTableSql);
            }
        }

        public void LoadTablesToTarget(bool paralellExection, int maxThreads)
        {
            if (paralellExection)
            {

                Parallel.ForEach(fetchTables, new ParallelOptions { MaxDegreeOfParallelism = maxThreads/10 }, (ft) =>
                {
                    Console.WriteLine($"Bulk inserting {ft.SourceTable} on thread {Thread.CurrentThread.ManagedThreadId}");
                    BulkInsert(ft, paralellExection, maxThreads);
                });
            }
            else
            {
                foreach (FetchTables f in fetchTables)
                {
                    BulkInsert(f, paralellExection, maxThreads);
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

        void BulkInsert(Shard shard, bool Incremental)
        {

            //string truncatesql = "TRUNCATE TABLE " + targetSchema + "." + sourceSchema + "_" + sourceTable;

            //DataAccess.ExecSqlNonQuery(truncatesql, targetConnStr);

            string bulkinsertsql = "bulk insert " + targetSchema + "." + shard.TableName + (Incremental?" ": "_tmp ") +
                                    "from '" + csvFolder + shard.Name + ".csv' " +
                                    "with( " +
                                     //"   format = 'csv', " +
                                     "   fieldterminator='" + csvSeparator + "'," +
                                     "   firstrow = 2, " +
                                     "   fieldquote = '\"', " +
                                     "   codepage = '65001' " +
                                    ")";
            TargetDataAccess.ExecSqlNonQuery(bulkinsertsql);

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
