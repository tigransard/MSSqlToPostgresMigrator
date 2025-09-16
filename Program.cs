using Microsoft.Data.SqlClient;
using Npgsql;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DbMigrator
{
    class AppConfig
    {
        public required string SqlServer { get; init; }
        public required string SqlDb { get; init; }
        public required string SqlUser { get; init; }
        public required string SqlPass { get; init; }
        public required string PgHost { get; set; }
        public required string PgPort { get; set; }
        public required string PgDb { get; set; }
        public required string PgUser { get; init; }
        public required string PgPass { get; init; }
        public bool VerifyTables { get; init; } = false;
        public bool VerifyData { get; init; } = false;
        public bool VerifyDirectly { get; init; } = true;
        public bool SkipExport { get; init; } = false;
        public string TypeMapFile { get; init; }
        public string ColumnsMapFile { get; init; }
        public string BaseFolder { get; private set; } = "migration_output";
        public string LogsTo { get; init; } = "both";

        public static AppConfig ParseArgs(string[] args)
        {
            var dict = args
                .Select((val, i) => (Key: val.StartsWith("--") ? val[2..] : null, Index: i))
                .Where(x => x.Key != null)
                .ToDictionary(x => x.Key!, x => args.Length > x.Index + 1 && !args[x.Index + 1].StartsWith("--") ? args[x.Index + 1] : "");

            var config = new AppConfig
            {
                SqlServer = dict.GetValueOrDefault("sql-server", "."),
                SqlDb = dict.GetValueOrDefault("sql-db", ""),
                SqlUser = dict.GetValueOrDefault("sql-user", ""),
                SqlPass = dict.GetValueOrDefault("sql-pass", ""),

                PgHost = dict.GetValueOrDefault("pg-host", "localhost"),
                PgPort = dict.GetValueOrDefault("pg-port", "5432"),
                PgDb = dict.GetValueOrDefault("pg-db", dict["sql-db"]),
                PgUser = dict.GetValueOrDefault("pg-user", Environment.GetEnvironmentVariable("PGUSER") ?? ""),
                PgPass = dict.GetValueOrDefault("pg-pass", Environment.GetEnvironmentVariable("PGPASSWORD") ?? ""),

                VerifyTables = args.Contains("--verify-tables"),
                VerifyData = args.Contains("--verify-data-only"),

                SkipExport = args.Contains("--skip-export"),
                TypeMapFile = dict.GetValueOrDefault("type-map"),
                ColumnsMapFile = dict.GetValueOrDefault("columns-map"),
                
                BaseFolder = dict.GetValueOrDefault("base-folder", "migration_output"),
                LogsTo = dict.GetValueOrDefault("logs-to", "both")
            };

            if (string.IsNullOrWhiteSpace(config.PgDb)) config.PgDb = config.SqlDb;
            if (string.IsNullOrWhiteSpace(config.PgHost)) config.PgHost = "localhost";
            if (string.IsNullOrWhiteSpace(config.PgPort)) config.PgPort = "5432";
            if (string.IsNullOrWhiteSpace(config.PgUser)) throw new Exception("Missing --pg-user and PGUSER is not set");
            if (string.IsNullOrWhiteSpace(config.PgPass)) throw new Exception("Missing --pg-pass and PGPASSWORD is not set");
            if (string.IsNullOrWhiteSpace(config.BaseFolder)) config.BaseFolder = "migration_output";
            // make sure logs are per db, if we have the default output
            if (config.BaseFolder == "migration_output")
            {
                config.BaseFolder += Path.DirectorySeparatorChar + config.SqlDb;
            }
            return config;
        }
    }

    partial class Program
    {
        static async Task Main(string[] args)
        {
            if (!args.Contains("--sql-db"))
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("  [--sql-server <host>] --sql-db <db> [--sql-user <user>] [--sql-pass <pass>]");
                Console.WriteLine("  [--pg-host <host>] [--pg-port <port>] [--pg-db <db>] [--pg-user <user>] [--pg-pass <pass>]");
                Console.WriteLine("  [--type-map <file>]");
                Console.WriteLine("  [--columns-map <file>]");
                Console.WriteLine("  [--base-folder <path>]");
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
