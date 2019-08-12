using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using Oracle.DataAccess.Client;
using System.Threading;
using System.Diagnostics;

namespace DLT
{
    class OracleSpoolSource : OracleSource
    {
        public OracleSpoolSource(string SqlServerSourceConnectionString) : base(SqlServerSourceConnectionString)
        {

        }

        public new void ExportTablesAsCsv(List<FetchTables> ft, bool parallelExecution, int maxThreads, string CsvFolder, string csvSeparator)
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

        public new void SaveShardAsCsv(Shard shard, string csvFolder, string csvSeparator)
        {
            SaveShardAsCsv(shard, csvFolder, csvSeparator, "Unicode");
        }

        public new void SaveShardAsCsv(Shard shard, string csvFolder, string csvSeparator, string encoding)
        {
            Console.WriteLine($"Spooling {shard.Name} on thread {Thread.CurrentThread.ManagedThreadId}");
            string path = csvFolder + shard.TableName + "\\";
            Directory.CreateDirectory(path);
            StreamWriter cmdFile = File.CreateText(path + "spool_" + shard.Name + ".cmd");

            cmdFile.WriteLine("SET NLS_LANG=SWEDISH_SWEDEN.WE8ISO8859P1");
            cmdFile.WriteLine("set NLS_NUMERIC_CHARACTERS=. ");
            cmdFile.WriteLine("sqlplus RMIENE/Skeppet1!@//sesrv145.intrum.net:1521/SCANP @"+path+"spool_"+shard.Name+".sql");

            cmdFile.Close();

            StreamWriter sqlFile = File.CreateText(path + "spool_" + shard.Name + ".sql");

            sqlFile.WriteLine("set markup csv on");
            sqlFile.WriteLine("set term off");
            sqlFile.WriteLine("set echo off");
            sqlFile.WriteLine("set trimspool on");
            sqlFile.WriteLine("set trimout on");
            sqlFile.WriteLine("set feedback off");
            sqlFile.WriteLine("Set serveroutput off");
            sqlFile.WriteLine("set heading off");
            sqlFile.WriteLine("set numformat fm999999999999999.99999");
            sqlFile.WriteLine("set arraysize 5000");
            sqlFile.WriteLine("spool " + path + shard.Name + ".csv");
            sqlFile.WriteLine(shard.Sql);
            sqlFile.WriteLine(";");
            sqlFile.WriteLine("spool off");
            sqlFile.WriteLine("exit");

            sqlFile.Close();
            string cmd =  @"/c " + path + "\\" + "spool_" + shard.Name + ".cmd";
            var fetchProcess = Process.Start("CMD.exe",cmd);
            Console.WriteLine(cmd);
            fetchProcess.WaitForExit();

        }

        public new void SaveTableAsCsv(FetchTables TableToFetch, string CsvFolder, string csvSeparator)
        {
            foreach (Shard shard in TableToFetch.Shards)
            {
                SaveShardAsCsv(shard, CsvFolder, csvSeparator);
            }
        }
    }
}
