using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    public class FetchTables
    {

        public string SourceSchema;
        public string SourceTable;
        public bool LoadToTarget = false;
        public bool Sharding = false;
        public string ShardMethod = "";
        public string ShardColumn = "";
        public string CreateTableSql = "";
        public string CreateTempTableSql = "";
        public string SwitchTableSql = "";
        public string DropTableSql = "";


        public FetchTables(string SourceSchema, string SourceTable, bool LoadToTarget)
        {
            this.SourceSchema = SourceSchema;
            this.SourceTable = SourceTable;
            this.LoadToTarget = LoadToTarget;
        }

        public FetchTables(string SourceSchema, string SourceTable)
        {
            this.SourceSchema = SourceSchema;
            this.SourceTable = SourceTable;
        }

        public List<Shard> Shards
        {
            get
            {
                // Return sql statement / statements (if sharding)
                List<Shard> shards = new List<Shard>();

                if (this.Sharding)
                {
                    switch (this.ShardMethod)
                    {
                        case "rightbase10":
                            for (int i = 0; i < 10; i++)
                            {
                                Shard s = new Shard("SELECT * FROM " + this.SourceSchema + "." + this.SourceTable + " WHERE RIGHT(CAST(" + this.ShardColumn + " as VARCHAR), 1) ='" + i.ToString() + "'", this.SourceSchema + "_" + this.SourceTable + "_" + i.ToString(), this.SourceSchema + "_" + this.SourceTable);
                                shards.Add(s);
                            }
                            break;
                    }
                }
                else
                {
                    shards.Add(new Shard("SELECT * FROM " + this.SourceSchema + "." + this.SourceTable, this.SourceSchema + "_" + this.SourceTable, this.SourceSchema + "_" + this.SourceTable));
                }

                return shards;
            }
        }
    }
}
