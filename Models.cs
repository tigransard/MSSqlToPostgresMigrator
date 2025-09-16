using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbMigrator
{
    record TableSchema(string Schema, string TableName, List<ColumnSchema> Columns);

    record ColumnSchema()
    {
        public string Name { get; set; }
        public string SqlServerType { get; set; }
        public bool IsNullable { get; set; }
        public string DefaultValue { get; set; }
        public string FullType { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsPersisted { get; set; }
        public bool IsComputed { get; set; }
        public int OrderNo { get; set; }
        public string ComputedDefinition { get; set; }
    };
}
