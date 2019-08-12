using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DLT
{
    public abstract class Source
    {
        public abstract List<FetchTables> LoadTablesFromConfig();
        public abstract List<FetchTables> LoadTablesFromConfig(int maxRowLimit);
        public abstract void ExportTablesAsCsv(List<FetchTables> ft, bool parallelExecution, int maxThreads, string CsvFolder, string csvSeparator);

    }
}
