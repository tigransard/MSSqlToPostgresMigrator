using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DbMigrator
{
    class AppConfig
    {
        public required string SourceType { get; init; }
        public required string SrcServer { get; set; }
        public string SrcPort { get; set; }
        public required string SrcDb { get; init; }
        public required string SrcUser { get; init; }
        public required string SrcPass { get; init; }
        public required string PgHost { get; set; }
        public required string PgPort { get; set; }
        public required string PgDb { get; set; }
        public required string PgUser { get; init; }
        public required string PgPass { get; init; }
        public bool Dbo2Public { get; init; } = false;
        public bool VerifyTables { get; init; } = false;
        public bool VerifyData { get; init; } = false;
        public bool VerifyDirectly { get; init; } = true;
        public bool SkipExport { get; init; } = false;
        public string TypeMapFile { get; init; }
        public string ColumnsMapFile { get; init; }
        public string QueriesFile { get; init; }
        public string BaseFolder { get; private set; } = "migration_output";
        public string LogsTo { get; private set; } = "both";

        public static AppConfig ParseArgs(string[] args)
        {
            var dict = args
                .Select((val, i) => (Key: val.StartsWith("--") ? val[2..] : null, Index: i))
                .Where(x => x.Key != null)
                .ToDictionary(x => x.Key!, x => args.Length > x.Index + 1 && !args[x.Index + 1].StartsWith("--") ? args[x.Index + 1] : "");

            var config = new AppConfig
            {
                SourceType = dict.GetValueOrDefault("src-type", "mssql"),
                SrcServer = dict.GetValueOrDefault("src-server", ""),
                SrcPort = dict.GetValueOrDefault("src-port", ""),
                SrcDb = dict.GetValueOrDefault("src-db", ""),
                SrcUser = dict.GetValueOrDefault("src-user", ""),
                SrcPass = dict.GetValueOrDefault("src-pass", ""),

                PgHost = dict.GetValueOrDefault("pg-host", ""),
                PgPort = dict.GetValueOrDefault("pg-port", ""),
                PgDb = dict.GetValueOrDefault("pg-db", ""),
                PgUser = dict.GetValueOrDefault("pg-user", Environment.GetEnvironmentVariable("PGUSER") ?? ""),
                PgPass = dict.GetValueOrDefault("pg-pass", Environment.GetEnvironmentVariable("PGPASSWORD") ?? ""),

                VerifyTables = args.Contains("--verify-tables"),
                VerifyData = args.Contains("--verify-data-only"),

                Dbo2Public = args.Contains("--dbo2public"),

                SkipExport = args.Contains("--skip-export"),
                TypeMapFile = dict.GetValueOrDefault("type-map"),
                ColumnsMapFile = dict.GetValueOrDefault("columns-map"),
                QueriesFile = dict.GetValueOrDefault("queries"),
                
                BaseFolder = dict.GetValueOrDefault("base-folder", ""),
                LogsTo = dict.GetValueOrDefault("logs-to", "")
            };

            if (string.IsNullOrWhiteSpace(config.SourceType)) config.SourceType = "mssql";
            config.SourceType = config.SourceType.ToLowerInvariant();
            if (config.SourceType != "mssql" && config.SourceType != "mysql")
            {
                throw new Exception("Invalid --src-type. Allowed values: mssql, mysql");
            }

            if (string.IsNullOrWhiteSpace(config.SrcServer)) config.SrcServer = ".";
            if (string.IsNullOrWhiteSpace(config.PgDb)) config.PgDb = config.SrcDb;
            if (string.IsNullOrWhiteSpace(config.PgHost)) config.PgHost = "localhost";
            if (string.IsNullOrWhiteSpace(config.PgPort)) config.PgPort = "5432";
            if (string.IsNullOrWhiteSpace(config.PgUser)) throw new Exception("Missing --pg-user and PGUSER is not set");
            if (string.IsNullOrWhiteSpace(config.PgPass)) throw new Exception("Missing --pg-pass and PGPASSWORD is not set");
            if (string.IsNullOrWhiteSpace(config.BaseFolder)) config.BaseFolder = "migration_output";
            if (string.IsNullOrWhiteSpace(config.LogsTo)) config.LogsTo = "both";
            // make sure logs are per db, if we have the default output
            if (config.BaseFolder == "migration_output")
            {
                config.BaseFolder += Path.DirectorySeparatorChar + config.SrcDb;
            }
            return config;
        }
    }

    partial class Program
    {
        static async Task Main(string[] args)
        {
            if (!args.Contains("--src-db"))
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("  [--src-server <host>] [--src-port <port>] --src-db <db> [--src-user <user>] [--src-pass <pass>]");
                Console.WriteLine("  [--src-type <mssql|mysql>]");
                Console.WriteLine("  [--pg-host <host>] [--pg-port <port>] [--pg-db <db>] [--pg-user <user>] [--pg-pass <pass>]");
                Console.WriteLine("  [--type-map <file>]");
                Console.WriteLine("  [--columns-map <file>]");
                Console.WriteLine("  [--queries <file>]");
                Console.WriteLine("  [--base-folder <path>]");
                Console.WriteLine("  [--dbo2public]");
                Console.WriteLine("  [--verify-tables]");
                Console.WriteLine("  [--verify-data-only]");
                Console.WriteLine("  [--logs-to <file|console|both>]");
                return;
            }

            var config = AppConfig.ParseArgs(args);
            await StartMigration(config);
        }
    }
}
