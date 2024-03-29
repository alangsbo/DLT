﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;
using System.Data;

namespace DLT
{
    public static class Log
    {
        public static long CsvBytesWritten = 0;
        public static DateTime CsvStartTime;
        public static DateTime CsvEndTime;
    }

    

    

    public class Program
    {
        static string sourceType = "";
        static string sourceConnStr = "";
        static string targetConnStr = "";
        static string logConnStr = "";
        static string csvSeparator = "";
        static bool skipCsv = false;
        static bool skipInsert = false;
        //static string targetSchema = "";
        static bool paralellExection = false;
        static int maxThreads = -1;
        static int testRowLimit = -1;
        
        static string csvFolder = "";

        static void Main(string[] args)
        {
            
            LoadConfig();
            Logger.Init(logConnStr);

            List<FetchTables> ft = null;
            Source source = null;
            if (sourceType == "sqlserver")
            {
                source = new SqlServerSource(sourceConnStr);
                ft = source.LoadTablesFromConfig(testRowLimit);
            } else if(sourceType == "oracle")
            {
                source = new OracleSource(sourceConnStr);
                ft = source.LoadTablesFromConfig(testRowLimit);

            } else if(sourceType == "oraclespool")
            {
                source = new OracleSpoolSource(sourceConnStr);
                ft = source.LoadTablesFromConfig(testRowLimit);
            }

            if (!skipCsv)
            {
                Log.CsvStartTime = DateTime.Now;
                //sqlSource.ExportTablesAsCsv(ft, paralellExection, maxThreads, csvFolder, csvSeparator);
                source.ExportTablesAsCsv(ft, paralellExection, maxThreads, csvFolder, csvSeparator);
                Log.CsvEndTime = DateTime.Now;
                try
                {
                    Console.WriteLine("Csv Load started: " + Log.CsvStartTime.ToLongTimeString());
                    Console.WriteLine("Csv Load started: " + Log.CsvEndTime.ToLongTimeString());
                    Console.WriteLine(Log.CsvBytesWritten / 1000000 + " MB loaded in " + (Log.CsvEndTime - Log.CsvStartTime).TotalSeconds + " seconds - " + (Log.CsvBytesWritten / 1000000) / (Log.CsvEndTime - Log.CsvStartTime).Seconds + " MB/s, " + ((Log.CsvBytesWritten / 1000000) / (Log.CsvEndTime - Log.CsvStartTime).TotalSeconds) * 8 + " MBPS");
                }
                catch (Exception ex) { }
            }

            if (!skipInsert)
            {
                Target t = new Target(targetConnStr, csvFolder, csvSeparator, ft);
                t.LoadTablesToTarget(paralellExection, maxThreads, sourceType == "oraclespool");
            }
           

            Console.WriteLine("Done...");

        }

        static void LoadConfig()
        {
 
            string[] lines = File.ReadAllLines("Config.txt");
            List<string> linesWithoutComments = new List<string>();

            foreach(string line in lines)
            {
                if(line.Length>2 && line.Substring(0, 2) != "//" && line.Substring(0,2) != "--")
                {
                    linesWithoutComments.Add(line);
                }
            }


            for (int i = 0; i < linesWithoutComments.Count; i++)
            {
                string line = linesWithoutComments[i];
                if (line.Split(':')[0] == "sourcetype")
                    sourceType = line.Split(':')[1].Trim();
                if (line.Split(':')[0] == "source")
                    sourceConnStr = line.Split(':')[1].Trim();
                if (line.Split(':')[0] == "dest")
                    targetConnStr = line.Split(':')[1].Trim();
                //if (line.Split(':')[0] == "targetschema")
                //    targetSchema = line.Split(':')[1].Trim();
                if (line.Split(':')[0] == "csvseparator")
                    csvSeparator = line.Split(':')[1].Trim();
                if (line.Split(':')[0] == "paralellexecution")
                    paralellExection = bool.Parse(line.Split(':')[1].Trim());
                if (line.Split(':')[0] == "skipcsv")
                    skipCsv = bool.Parse(line.Split(':')[1].Trim());
                if (line.Split(':')[0] == "skipinsert")
                    skipInsert = bool.Parse(line.Split(':')[1].Trim());
                if (line.Split(':')[0] == "maxthreads")
                    maxThreads = int.Parse(line.Split(':')[1].Trim());
                if (line.Split(':')[0] == "limitrowsfortest")
                    testRowLimit = int.Parse(line.Split(':')[1].Trim());
                if (line.Split(':')[0] == "log")
                    logConnStr = line.Split(':')[1].Trim();
                if (line.Split(": ".ToCharArray())[0].ToString() == "csvfolder")
                {
                    csvFolder = line.Split(": ".ToCharArray(), 2)[1].ToString().Trim();
                    if (csvFolder.ToCharArray()[csvFolder.Length - 1] != '\\')
                        csvFolder += "\\";
                }


                // Initialie Target Data Acccess
                TargetDataAccess.TargetConnStr = targetConnStr;
            }
        }
    }
}
