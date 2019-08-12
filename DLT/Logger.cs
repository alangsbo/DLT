using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    public static class Logger
    {
        public static string logConnectionString = "";
        public static string batchId = "";
        public static DateTime startTs;

        public static void Init(string LogConnectionString)
        {
            logConnectionString = LogConnectionString;
           

            // Check if log table exists, otherwise create it
            string Sql = "select case when OBJECT_ID('dbo.DltLog', 'U') is not null then 'true' else 'false' end";
            bool logTableExists = bool.Parse(GetSingleValue(Sql).ToString());
            if (!logTableExists)
            {
                string createLogTableSql =
                    " CREATE TABLE dbo.DltLog( " +
                    "    BatchId nvarchar(128) NULL," +
                    "    StepId nvarchar(128) NOT NULL," +
                    "    LogTimeStamp datetime NULL default(getdate())," +
                    "    BatchStart datetime NOT NULL," +
                    "    StepName varchar(500) NOT NULL," +
                    "    StepInfo varchar(500) NOT NULL," +
                    "    StartTime datetime NOT NULL," +
                    "    EndTime datetime NULL," +
                    "    ErrorTime datetime NULL," +
                    "    ErrorMessage varchar(8000) NULL" +
                    " ) ";

                ExecSqlNonQuery(createLogTableSql);
            }

            batchId = Guid.NewGuid().ToString();
            startTs = DateTime.Now;
        }

        //public Log
        
        public static void LogStepStart(string StepId, string StepName, string StepInfo)
        {
            string sql = "INSERT INTO dbo.DltLog(BatchId, StepId, LogTimeStamp, BatchStart, StepName, StepInfo, StartTime) ";
            sql += "VALUES('"+batchId+ "','"+StepId+ "','"+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','"+startTs+"','"+StepName+"','"+StepInfo+"','"+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')";
            ExecSqlNonQuery(sql);
        }

        public static void LogStepEnd(string StepId)
        {
            string sql = "UPDATE dbo.DltLog set EndTime='" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "' WHERE StepId='" + StepId + "'";
            ExecSqlNonQuery(sql);
        }

        private static bool ExecSqlNonQuery(string sql)
        {
            SqlConnection conn = new SqlConnection(logConnectionString);
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = 0;
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
                return false;
            }

            return true;
        }

        private static object GetSingleValue(string sql)
        {
            SqlConnection conn = new SqlConnection(logConnectionString);
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            object result = null;
            try
            {
                conn.Open();
                result = cmd.ExecuteScalar();
                conn.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(sql);
            }
            return result;
        }
    }
}
