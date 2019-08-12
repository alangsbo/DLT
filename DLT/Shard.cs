using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    public class Shard
    {
        public string Sql;
        public string Name;
        public string TableName;
        public string TargetSchema;
        
        public Shard(string Sql, string Name, string TableName, string TargetSchema)
        {
            this.Sql = Sql;
            this.Name = Name;
            this.TableName = TableName;
            this.TargetSchema = TargetSchema;
        }
    }
}
