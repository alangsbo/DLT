using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace DLT
{
    public abstract class SourceDataAccess
    {
        protected string sourceConnStr;
        protected string fetchMetadataSql;

        public abstract DataSet GetCreateTableMetaDataSet();
   
    }

    public class SqlServerSourceDataAccess : SourceDataAccess
    {
       

        public SqlServerSourceDataAccess(string ConnectionString)
        {
            this.sourceConnStr = ConnectionString;
            this.fetchMetadataSql = File.ReadAllText("SqlServerMetaDataFetchTemplate.txt");
        }

        public override DataSet GetCreateTableMetaDataSet()
        {
            DataSet ds = GetDataSet(this.fetchMetadataSql);
            return ds;
        }

       

        public DataSet GetDataSet(string SQL)
        {
            SqlConnection conn = new SqlConnection(this.sourceConnStr);
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

    }
}
