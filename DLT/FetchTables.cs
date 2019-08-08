﻿using System;
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
        public bool Incremental = false;
        public string IncrementalColumn = "";
        public string IncrementalColumnType = "";


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

                // If table is incremental, get the max loaded value for the incremental column from Target
                string incrWhere = "";
                if(this.Incremental)
                {
                    string incrSql = "SELECT MAX(" + this.IncrementalColumn + ") FROM dlt." + this.SourceSchema + "_" + this.SourceTable;
                    object maxValueInTarget = TargetDataAccess.GetSingleValue(incrSql);

                    // If no object is returned, table is probably empty, 
                    if (maxValueInTarget.ToString() == "")
                    {
                        incrWhere = "1=1";
                    }
                    else
                    {
                        incrWhere = this.IncrementalColumn + ">" + (this.IncrementalColumnType.ToLower()=="date"?"'"+maxValueInTarget.ToString()+ "'": maxValueInTarget.ToString());
                    }
                    
                }

                if (this.Sharding)
                {
                    switch (this.ShardMethod)
                    {
                        case "rightbase10":
                            for (int i = 0; i < 10; i++)
                            {
                                Shard s = new Shard("SELECT * FROM " + this.SourceSchema + "." + this.SourceTable + " WHERE RIGHT(CAST(" + this.ShardColumn + " as VARCHAR), 1) ='" + i.ToString() + "'" + (this.Incremental?" AND "+incrWhere:""), this.SourceSchema + "_" + this.SourceTable + "_" + i.ToString(), this.SourceSchema + "_" + this.SourceTable);
                                shards.Add(s);
                            }
                            break;
                    }
                }
                else
                {
                    shards.Add(new Shard("SELECT * FROM " + this.SourceSchema + "." + this.SourceTable, this.SourceSchema + "_" + this.SourceTable, this.SourceSchema + "_" + this.SourceTable + (this.Incremental ? " WHERE " + incrWhere : "")));
                }

                return shards;
            }
        }
    }
}