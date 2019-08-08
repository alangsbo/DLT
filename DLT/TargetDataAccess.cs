using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace DLT
{
    public static class TargetDataAccess
    {
        public static string TargetConnStr = "";

        public static void ExecSqlNonQuery(string sql)
        {
            SqlConnection conn = new SqlConnection(TargetConnStr);
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

        public static object GetSingleValue(string sql)
        {
            SqlConnection conn = new SqlConnection(TargetConnStr);
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
