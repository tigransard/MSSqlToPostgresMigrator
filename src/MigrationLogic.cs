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

namespace DbMigrator;

partial class Program
{
    private static async Task StartMigration(AppConfig config)
    {
        if (!string.IsNullOrWhiteSpace(config.BaseFolder))
        {
            if (!Directory.Exists(config.BaseFolder))
            {
                Directory.CreateDirectory(config.BaseFolder);
            }
        }

        var logConfigurator = new LoggerConfiguration()
            .MinimumLevel.Information();

        var logFileFullPath = Path.Combine(config.BaseFolder, "_migration.log");

        logConfigurator = config.LogsTo switch
        {
            "file" => logConfigurator.WriteTo.File(logFileFullPath, rollingInterval: RollingInterval.Infinite),
            "console" => logConfigurator.WriteTo.Console(),
            _ => logConfigurator
                .WriteTo.Console()
                .WriteTo.File(logFileFullPath, rollingInterval: RollingInterval.Infinite)
        };

        Log.Logger = logConfigurator.CreateLogger();

        string sqlConnStr = $"Server={config.SqlServer}{(string.IsNullOrEmpty(config.SqlPort) ? "" : "," + config.SqlPort)};Database={config.SqlDb};Encrypt=False;";
        sqlConnStr += !string.IsNullOrEmpty(config.SqlUser) ? $"User Id={config.SqlUser};Password={config.SqlPass};" : "Trusted_Connection=True;";

        string pgConnStr = $"Host={config.PgHost};Port={config.PgPort};Database={config.PgDb};Username={config.PgUser};Password={config.PgPass};";

        if (config.VerifyTables || config.VerifyData)
        {
            if (config.VerifyTables)
            {
                Log.Information("Running in VERIFY-TABLES mode — no data will be migrated.");
                if (await RunVerifyTablesMode(config, sqlConnStr, pgConnStr, config.BaseFolder))
                {
                    Log.Information("ERROR in VERIFY-TABLES mode");
                    throw new Exception("VERIFY-TABLES failed");
                }
            }

            if (config.VerifyData)
            {
                Log.Information("Running in VERIFY-DATA mode — no data will be migrated.");
                if (await RunVerifyDataMode(sqlConnStr, pgConnStr, config.BaseFolder, config.QueriesFile))
                {
                    Log.Information("ERROR in VERIFY-DATA mode");
                    throw new Exception("VERIFY-DATA failed");
                }
            }

            return;
        }

        if (!string.IsNullOrWhiteSpace(config.TypeMapFile))
        {
            LoadTypeMapFromFile(config.TypeMapFile);
        }

        var computedColumnsMappings = new Dictionary<string, string>();
        if (!string.IsNullOrWhiteSpace(config.ColumnsMapFile))
        {
            computedColumnsMappings = LoadColumnsMapFromFile(config.ColumnsMapFile);
        }

        var rowCounts = new Dictionary<string, int>();
        var identityMaxValues = new Dictionary<string, (string ColumnName, long MaxValue)>();
        var boolColumns = new Dictionary<string, HashSet<string>>();  // "schema.table" => set of boolean column names
        var createTableStatements = new List<string>();
        var pkUqConstraints = new List<string>();
        var indexStatements = new List<string>();
        var foreignKeys = new List<string>();

        Log.Information("Extracting schema...");
        var tables = await ExtractSqlServerSchema(sqlConnStr);
        ValidateSqlTypes(tables);

        foreach (var table in tables)
        {
            var bools = table.Columns
                .Where(c => c.SqlServerType.Equals("bit", StringComparison.OrdinalIgnoreCase))
                .Select(c => c.Name)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            boolColumns[$"{table.Schema}.{table.TableName}"] = bools;
        }


        Log.Information("Counting source table rows...");
        await using (var conn = new SqlConnection(sqlConnStr))
        {
            await conn.OpenAsync();

            foreach (var table in tables)
            {
                string fullName = $"{table.Schema}.{table.TableName}";
                var (sourceCount, identityCol, maxId) = await GetTableStatsSqlServer(conn, table, true);
                rowCounts[fullName] = sourceCount;

                if (identityCol != null && maxId.HasValue)
                    identityMaxValues[fullName] = (identityCol, maxId.Value);
            }
        }

        if (!config.SkipExport)
        {
            Log.Information("Extracting constraints, indexes, and foreign keys...");
            await ExtractConstraintsAndIndexes(sqlConnStr, boolColumns, pkUqConstraints, indexStatements, foreignKeys, config.Dbo2Public);

            await File.WriteAllTextAsync(Path.Combine(config.BaseFolder, "__constraints.sql"), string.Join("\n\n", pkUqConstraints));
            await File.WriteAllTextAsync(Path.Combine(config.BaseFolder, "__indexes.sql"), string.Join("\n\n", indexStatements));
            await File.WriteAllTextAsync(Path.Combine(config.BaseFolder, "__foreign_keys.sql"), string.Join("\n\n", foreignKeys));

            Log.Information("Exporting data to CSV...");
            foreach (var table in tables)
                await ExportTableToCsv(sqlConnStr, table, config.BaseFolder);
        }


        Log.Information("Creating PostgreSQL tables...");
        await CreatePostgresTables(pgConnStr, tables, computedColumnsMappings, createTableStatements, config.Dbo2Public);

        await File.WriteAllTextAsync(Path.Combine(config.BaseFolder, "__tables.sql"), string.Join("\n\n\n", createTableStatements));


        Log.Information("Importing data into PostgreSQL...");
        foreach (var table in tables)
        {
            await ImportCsvToPostgres(pgConnStr, table, config.BaseFolder, config.Dbo2Public);
        }

        Log.Information("Resetting identity column values...");
        await using (var conn = new NpgsqlConnection(pgConnStr))
        {
            await conn.OpenAsync();

            foreach (var table in tables)
            {
                string fullName = $"{table.Schema}.{table.TableName}";
                if (identityMaxValues.TryGetValue(fullName, out var ident))
                {
                    var schema = table.Schema;
                    if(config.Dbo2Public && schema.Equals("dbo", StringComparison.InvariantCultureIgnoreCase)) { schema = "public"; }

                    string sql =
                        $"SELECT setval(pg_get_serial_sequence('\"{schema}\".\"{table.TableName}\"', '{ident.ColumnName}'), {ident.MaxValue + 1}); ";

                    await using var cmd = new NpgsqlCommand(sql, conn);
                    await cmd.ExecuteNonQueryAsync();

                    Log.Information($"Identity reset for {fullName}.{ident.ColumnName} to {ident.MaxValue + 1}");
                }
            }
        }

        Log.Information("Applying PRIMARY/UNIQUE constraints...");
        await ApplySqlBatch(pgConnStr, pkUqConstraints);

        Log.Information("Creating indexes...");
        await ApplySqlBatch(pgConnStr, indexStatements);

        Log.Information("Applying foreign key constraints...");
        await ApplySqlBatch(pgConnStr, foreignKeys);

        Log.Information("Verifying row counts...");
        var reportLines = new List<string>();
        reportLines.Add($"Counts report - {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        reportLines.Add("------------------------------------------------------");

        await using (var conn = new NpgsqlConnection(pgConnStr))
        {
            await conn.OpenAsync();

            foreach (var table in tables)
            {
                string fullName = $"{table.Schema}.{table.TableName}";
                long sourceCount = (long)rowCounts[fullName];
                long destCount = await GetRowCountInPostgres(conn, table, config.Dbo2Public);

                string status = destCount == sourceCount ? "✅ MATCH" : "❌ MISMATCH";
                if (destCount != sourceCount)
                    Log.Error($"❌ Row count mismatch: {fullName} — source={sourceCount}, destination={destCount}");
                /*else
                    Log.Information($"✅ Row count match: {fullName} = {sourceCount}");*/

                reportLines.Add($"{fullName,-80} | source: {sourceCount,-10} | destination: {destCount,-10} | {status}");
            }

            await File.WriteAllLinesAsync(Path.Combine(config.BaseFolder, "_rows_count_report.log"), reportLines);
        }

        if (config.VerifyDirectly)
        {
            Log.Information("Running VERIFY-TABLES");
            if (await RunVerifyTablesMode(config, sqlConnStr, pgConnStr, config.BaseFolder))
            {
                throw new Exception("VERIFY-TABLES failed");
            }
            Log.Information("Running VERIFY-DATA");
            if (await RunVerifyDataMode(sqlConnStr, pgConnStr, config.BaseFolder, config.QueriesFile))
            {
                throw new Exception("VERIFY-DATA failed");
            }
        }

        Log.Information("✅ Migration complete. Logs and reports written to disk.");
    }

    static async Task<List<TableSchema>> ExtractSqlServerSchema(string connStr, bool needColumns = true)
    {
        var result = new List<TableSchema>();
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();

        var tableCmd = new SqlCommand(@"SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME", conn);
        var reader = await tableCmd.ExecuteReaderAsync();
        var tables = new List<(string Schema, string Table)>();
        while (await reader.ReadAsync())
            tables.Add((reader.GetString(0), reader.GetString(1)));
        await reader.CloseAsync();

        if (needColumns)
        {
            var columnsCmd = new SqlCommand(@"
                    SELECT 
                        c.name AS COLUMN_NAME,
                        t.name AS DATA_TYPE,
                        c.is_nullable AS IS_NULLABLE,
                        dc.definition AS COLUMN_DEFAULT,
                        t.name + 
                            COALESCE('(' + 
                                CASE 
                                    WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar') AND c.max_length > 0 
                                        THEN CAST(
                                            CASE 
                                                WHEN t.name IN ('nchar', 'nvarchar') THEN c.max_length / 2
                                                ELSE c.max_length
                                            END AS VARCHAR)
                                    WHEN t.name IN ('decimal', 'numeric') 
                                        THEN CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR)
                                    ELSE NULL
                                END + ')', '') AS FULL_TYPE,
                        COLUMNPROPERTY(c.object_id, c.name, 'IsIdentity') AS IS_IDENTITY,
                        cc.is_persisted,
                        c.is_computed,
                        cc.definition AS ComputedDefinition,
                        c.column_id
                    FROM 
                        sys.columns AS c
                        INNER JOIN sys.tables AS tbl ON c.object_id = tbl.object_id
                        INNER JOIN sys.schemas AS s ON tbl.schema_id = s.schema_id
                        INNER JOIN sys.types AS t ON c.user_type_id = t.user_type_id
                        LEFT JOIN sys.computed_columns AS cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
                        LEFT JOIN sys.default_constraints AS dc ON c.default_object_id = dc.object_id
                    WHERE s.name = @Schema and tbl.name = @Table
                    ORDER BY c.column_id 
                    ", conn);

            foreach (var (schema, table) in tables)
            {
                try
                {
                    columnsCmd.Parameters.Clear();
                    columnsCmd.Parameters.AddWithValue("@Schema", schema);
                    columnsCmd.Parameters.AddWithValue("@Table", table);
                    var colReader = await columnsCmd.ExecuteReaderAsync();
                    var columns = new List<ColumnSchema>();
                    while (await colReader.ReadAsync())
                    {
                        columns.Add(new ColumnSchema()
                        {
                            Name = colReader.GetString(0),
                            SqlServerType = colReader.GetString(1),
                            IsNullable = colReader.GetBoolean(2),
                            DefaultValue = colReader.IsDBNull(3) ? null : colReader.GetString(3),
                            FullType = colReader.IsDBNull(4) ? null : colReader.GetString(4),
                            IsIdentity = !colReader.IsDBNull(5) && colReader.GetInt32(5) == 1,
                            IsPersisted = !colReader.IsDBNull(6) && colReader.GetBoolean(6),
                            IsComputed = !colReader.IsDBNull(7) && colReader.GetBoolean(7),
                            ComputedDefinition = colReader.IsDBNull(8) ? null : colReader.GetString(8),
                            OrderNo = colReader.GetInt32(9)
                        });
                    }

                    await colReader.CloseAsync();
                    result.Add(new TableSchema(schema, table, columns));
                }
                catch (Exception ex)
                {
                    Log.Error(ex, $"Can't read columns from table {schema}.{table}");
                    throw;
                }
            }
        }
        else
        {
            foreach (var (schema, table) in tables)
            {
                result.Add(new TableSchema(schema, table, null));
            }
        }

        return result;
    }

    static async Task<(int RowCount, string IdentityColumn, long? MaxIdentity)> GetTableStatsSqlServer(SqlConnection conn, TableSchema table, bool needMax)
    {
        await using var countCmd = new SqlCommand($"SELECT COUNT(*) FROM [{table.Schema}].[{table.TableName}]", conn);
        var count = (int)await countCmd.ExecuteScalarAsync();

        if (needMax && table.Columns != null)
        {
            var identityCol = table.Columns.FirstOrDefault(c => c.IsIdentity)?.Name;
            if (identityCol != null)
            {
                await using var maxCmd = new SqlCommand($"SELECT MAX([{identityCol}]) FROM [{table.Schema}].[{table.TableName}]", conn);
                var maxVal = await maxCmd.ExecuteScalarAsync();
                long max = (maxVal != DBNull.Value) ? Convert.ToInt64(maxVal) : 0;
                return (count, identityCol, max);
            }
        }

        return (count, null, null);
    }

    static async Task<long> GetRowCountInPostgres(NpgsqlConnection conn, TableSchema table, bool dbo2public = false)
    {
        var schema = table.Schema;

        try
        {
            if (dbo2public && schema.Equals("dbo", StringComparison.InvariantCultureIgnoreCase)) { schema = "public"; }

            await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM \"{schema}\".\"{table.TableName}\"", conn);
            var count = await cmd.ExecuteScalarAsync();
            return (long)count;
        }
        catch (Exception ex)
        {
            Log.Error(ex, $"can't calculate rows count in destination database for {schema}.{table.TableName}");
        }

        return 0;
    }

    static void LoadTypeMapFromFile(string filePath)
    {
        if (!File.Exists(filePath))
        {
            Log.Warning($"Types mapping file '{filePath}' not found. Using built-in type map.");
            return;
        }

        var typeMapJson = File.ReadAllText(filePath);
        var externalMap = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(typeMapJson);
        if (externalMap != null)
        {
            foreach (var (key, value) in externalMap)
                TypeMap[key.ToLowerInvariant()] = value;
            Log.Information($"Loaded {externalMap.Count} custom type mappings from {filePath}");
        }
    }

    static Dictionary<string, string> LoadColumnsMapFromFile(string filePath)
    {
        Dictionary<string, string> externalMap = null;

        if (!File.Exists(filePath))
        {
            Log.Warning($"Columns mapping file '{filePath}' not found. Using built-in converter.");
        }
        else
        {
            var typeMapJson = File.ReadAllText(filePath);
            externalMap = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(typeMapJson);
        }

        return externalMap ?? new Dictionary<string, string>();
    }

    static void ValidateSqlTypes(List<TableSchema> tables)
    {
        var missingColumns = tables
            .SelectMany(t => t.Columns, (t, c) => new { t.Schema, t.TableName, Column = c })
            .Where(x => !TypeMap.ContainsKey(x.Column.SqlServerType.ToLowerInvariant()))
            .Select(x => new { x.Schema, x.TableName, ColumnName = x.Column.Name, SqlType = x.Column.SqlServerType })
            .ToList();

        if (missingColumns.Any())
        {
            foreach (var col in missingColumns)
                Log.Error($"❌ Missing type mapping for column: {col.Schema}.{col.TableName}.{col.ColumnName} (SQL Server type: {col.SqlType})");

            throw new Exception("Migration aborted due to unmapped SQL Server data types.");
        }
    }


    static Dictionary<string, string> TypeMap = new()
    {
        ["int"] = "INTEGER",
        ["bigint"] = "BIGINT",
        ["bit"] = "BOOLEAN",
        ["varchar"] = "TEXT",
        ["nvarchar"] = "TEXT",
        ["text"] = "TEXT",
        ["datetime"] = "TIMESTAMP",
        ["smalldatetime"] = "TIMESTAMP",
        ["decimal"] = "DECIMAL",
        ["numeric"] = "DECIMAL",
        ["float"] = "DOUBLE PRECISION",
        ["uniqueidentifier"] = "UUID"
    };

    static string MapSqlServerColumnTypeToPostgreSql(ColumnSchema column)
    {
        string baseType = column.SqlServerType.ToLowerInvariant();
        string fullType = column.FullType?.ToLowerInvariant();
        string pgType = "TEXT";

        if (baseType is "varchar" or "nvarchar")
        {
            if (fullType?.Contains("max") == true)
                pgType = "TEXT";
            else if (Regex.Match(fullType ?? "", @"\((\d+)\)") is { Success: true } m)
                pgType = $"VARCHAR({m.Groups[1].Value})";
        }
        else if (baseType is "char" or "nchar")
        {
            if (fullType?.Contains("max") == true)
                pgType = "TEXT";
            else if (Regex.Match(fullType ?? "", @"\((\d+)\)") is { Success: true } m)
                pgType = $"CHAR({m.Groups[1].Value})";
        }
        else if (baseType is "decimal" or "numeric")
        {
            var match = Regex.Match(fullType ?? "", @"\((\d+),(\d+)\)");
            pgType = match.Success ? $"DECIMAL({match.Groups[1].Value},{match.Groups[2].Value})" : "DECIMAL";
        }
        else
        {
            pgType = TypeMap.GetValueOrDefault(baseType, null);
            if (pgType == null)
                throw new InvalidOperationException($"No PostgreSQL type mapping defined for SQL Server type: {baseType}");
        }

        var defaultClause = !string.IsNullOrWhiteSpace(column.DefaultValue)
            ? $" DEFAULT {TranslateSqlServerDefault(column.DefaultValue, pgType.ToUpper())}"
            : "";

        return $"{pgType}{defaultClause}";
    }

    static string TranslateSqlServerDefault(string value, string pgType)
    {
        var result = TrimBalancedParentheses(value.Trim()) switch
        {
            "getdate()" => "CURRENT_TIMESTAMP",
            "sysutcdatetime()" => "(now() at time zone 'utc')",
            "CONVERT([bigint],(0))" => "0",
            "CONVERT([bigint],(-1))" => "-1",
            "CONVERT([bit],(0))" => "0",
            "CONVERT([bit],(1))" => "1",
            "N''" => "''",
            "N'S'" => "'S'",
            "N'/-1/'" => "'/-1/'",
            var raw => raw
        };

        if (pgType == "BOOLEAN" || pgType == "BOOL")
        {
            if (result == "1") result = "true";
            else if (result == "0") result = "false";
        }

        return result;
    }

    static string TrimBalancedParentheses(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return input;

        int start = 0;
        int end = input.Length;

        while (start < end && input[start] == '(' && input[end - 1] == ')')
        {
            start++;
            end--;
        }

        return input.Substring(start, end - start).Trim();
    }

    static async Task CreatePostgresTables(
        string pgConnStr,
        List<TableSchema> tables,
        Dictionary<string, string> computedColumns,
        List<string> createTableStatements,
        bool dbo2Public = false)
    {
        await using var pgConn = new NpgsqlConnection(pgConnStr);
        await pgConn.OpenAsync();

        var schemas = tables.Select(a => a.Schema).Distinct().ToList();
        if(dbo2Public)
        {
            schemas.Remove("dbo");
        }
        foreach (var schema in schemas)
        {
            await new NpgsqlCommand($"CREATE SCHEMA IF NOT EXISTS \"{schema}\";", pgConn).ExecuteNonQueryAsync();
        }

        foreach (var table in tables)
        {
            var colDefs = string.Join(",\n", table.Columns.Select(c =>
            {
                var pgType = MapSqlServerColumnTypeToPostgreSql(c);
                var identity = c.IsIdentity ? " GENERATED ALWAYS AS IDENTITY (START 1 INCREMENT 1)" : "";
                var computed = c.IsComputed ? $" GENERATED ALWAYS AS ({ConvertComputed(table.Schema, table.TableName, c.Name, c.ComputedDefinition, computedColumns)}) {(c.IsPersisted ? " STORED" : "")}" : "";
                var notNull = c.IsNullable ? "" : " NOT NULL";
                return $"\"{c.Name}\" {pgType}{identity}{computed}{notNull}";
            }));

            var schema = table.Schema;
            if(dbo2Public && schema.Equals("dbo", StringComparison.InvariantCultureIgnoreCase)) { schema = "public"; }

            var fullTableName = $"\"{schema}\".\"{table.TableName}\"";
            var createSql = $"CREATE TABLE IF NOT EXISTS {fullTableName} (\n{colDefs});";
            createTableStatements.Add(createSql);

            try
            {
                await using var cmd = new NpgsqlCommand(createSql, pgConn);
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Log.Error(ex, $"Can't create table {schema}.{table.TableName} \n\n {createSql}");

                throw;
            }

            Log.Information($"Table created: {schema}.{table.TableName}");
        }
    }


    static async Task ExtractConstraintsAndIndexes(
        string sqlConnStr,
        Dictionary<string, HashSet<string>> boolColumns,
        List<string> pkUq,
        List<string> indexes,
        List<string> foreignKeys,
        bool dbo2Public = false)
    {
        await using var conn = new SqlConnection(sqlConnStr);
        await conn.OpenAsync();

        var constraintQuery = @"
        SELECT 
            tc.TABLE_SCHEMA,
            tc.TABLE_NAME, 
            tc.CONSTRAINT_NAME, 
            tc.CONSTRAINT_TYPE, 
            STRING_AGG(kcu.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION) AS Columns
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
          ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME and tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
        WHERE tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
        GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE";

        await using var constraintCmd = new SqlCommand(constraintQuery, conn);
        var reader = await constraintCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var schema = reader.GetString(0);
            var table = reader.GetString(1);
            var constraintName = reader.GetString(2);
            var constraintType = reader.GetString(3);
            var columns = reader.GetString(4);

            if(dbo2Public && schema.Equals("dbo", StringComparison.InvariantCultureIgnoreCase)) { schema = "public"; }

            var typeSql = constraintType == "PRIMARY KEY" ? "PRIMARY KEY" : "UNIQUE";
            var sql =
                $"ALTER TABLE \"{schema}\".\"{table}\" ADD CONSTRAINT \"{constraintName}\" {typeSql} ({string.Join(", ", columns.Split(',').Select(c => $"\"{c.Trim()}\""))});";
            pkUq.Add(sql);
        }

        await reader.CloseAsync();

        var indexQuery = @"
        SELECT 
            s.name AS SchemaName,
            t.name AS TableName,
            i.name AS IndexName,
            STRING_AGG(CASE WHEN ic.is_included_column = 0 THEN QUOTENAME(c.name, '""') + 
                CASE ic.is_descending_key WHEN 1 THEN ' DESC' WHEN 0 THEN ' ASC' ELSE '' END ELSE NULL END, ', ') WITHIN GROUP (ORDER BY ic.index_column_id) AS KeyColumns,
            STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN QUOTENAME(c.name, '""') ELSE NULL END, ', ') WITHIN GROUP (ORDER BY ic.index_column_id) AS IncludeColumns,
            i.filter_definition,
            i.is_unique
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        JOIN sys.tables t ON t.object_id = i.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE i.is_primary_key = 0 AND i.is_unique_constraint = 0 AND i.is_disabled = 0
        GROUP BY s.name, t.name, i.name, i.filter_definition, i.is_unique";

        await using var indexCmd = new SqlCommand(indexQuery, conn);
        reader = await indexCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var schema = reader.GetString(0);
            var table = reader.GetString(1);
            var indexName = reader.GetString(2);
            var isUnique = reader.GetBoolean(6);
            var keyCols = reader.IsDBNull(3) ? "" : reader.GetString(3);
            var includeCols = reader.IsDBNull(4) ? "" : reader.GetString(4);
            var filter = reader.IsDBNull(5) ? null : reader.GetString(5);

            if (dbo2Public && schema.Equals("dbo", StringComparison.InvariantCultureIgnoreCase)) { schema = "public"; }

            var baseSql = $"CREATE {(isUnique ? "UNIQUE" : "")} INDEX \"{indexName}\" ON \"{schema}\".\"{table}\" ({keyCols})";
            if (!string.IsNullOrWhiteSpace(includeCols))
                baseSql += $" INCLUDE ({string.Join(", ", includeCols.Split(',').Select(c => c.Trim()))})";

            if (!string.IsNullOrWhiteSpace(filter))
            {
                var translatedFilter = TranslateSqlServerExpressionToPg(filter, schema, table, boolColumns, out bool hasUnmapped);
                if (hasUnmapped)
                    indexes.Add($"-- {baseSql} WHERE ({filter});");
                else
                    indexes.Add($"{baseSql} WHERE ({translatedFilter});");
            }
            else
            {
                indexes.Add($"{baseSql};");
            }
        }

        await reader.CloseAsync();

        var fkQuery = @"
    select 
		fk.name as CONSTRAINT_NAME,
		schema_name(fk_tab.schema_id) CONSTRAINT_SCHEMA, 
		fk_tab.name as TABLE_NAME,
		schema_name(pk_tab.schema_id) REFERENCED_SCHEMA, 
		pk_tab.name as REFERENCED_TABLE,
		string_agg(fk_col.name, ', ') WITHIN GROUP (ORDER BY fk_cols.constraint_column_id) as fk_columns,
		string_agg(pk_col.name, ', ') WITHIN GROUP (ORDER BY fk_cols.constraint_column_id) as pk_columns,
		fk.delete_referential_action,
		fk.update_referential_action
	from sys.foreign_keys fk
		inner join sys.tables fk_tab
			on fk_tab.object_id = fk.parent_object_id
		inner join sys.tables pk_tab
			on pk_tab.object_id = fk.referenced_object_id
		inner join sys.foreign_key_columns fk_cols
			on fk_cols.constraint_object_id = fk.object_id
		inner join sys.columns fk_col
			on fk_col.column_id = fk_cols.parent_column_id
			and fk_col.object_id = fk_tab.object_id
		inner join sys.columns pk_col
			on pk_col.column_id = fk_cols.referenced_column_id
			and pk_col.object_id = pk_tab.object_id
	group by fk_tab.schema_id, fk_tab.name, pk_tab.schema_id, pk_tab.name, fk.name, fk.delete_referential_action, fk.update_referential_action";

        await using var fkCmd = new SqlCommand(fkQuery, conn);
        reader = await fkCmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var constraintName = reader.GetString(0);
            var schema = reader.GetString(1);
            var table = reader.GetString(2);
            var refSchema = reader.GetString(3);
            var refTable = reader.GetString(4);
            var fkCols = reader.GetString(5);
            var pkCols = reader.GetString(6);
            var deleteAction = reader.GetByte(7);
            var updateAction = reader.GetByte(8);

            if (dbo2Public)
            {
                if (schema.Equals("dbo", StringComparison.InvariantCultureIgnoreCase)) { schema = "public"; }
                if (refSchema.Equals("dbo", StringComparison.InvariantCultureIgnoreCase)) { refSchema = "public"; }
            }

            var sql =
                $"ALTER TABLE \"{schema}\".\"{table}\" ADD CONSTRAINT \"{constraintName}\" " +
                $"  FOREIGN KEY ({string.Join(", ", fkCols.Split(',').Select(c => $"\"{c.Trim()}\""))}) " +
                $"  REFERENCES \"{refSchema}\".\"{refTable}\" ({string.Join(", ", pkCols.Split(',').Select(c => $"\"{c.Trim()}\""))}) " +
                $"    ON DELETE {(deleteAction == 1 ? "CASCADE" : "NO ACTION")} " +
                $"    ON UPDATE {(updateAction == 1 ? "CASCADE" : "NO ACTION")};";
            foreignKeys.Add(sql);
        }

        await reader.CloseAsync();
    }

    static async Task ExportTableToCsv(string sqlConnStr, TableSchema table, string baseFolder)
    {
        int flushThreshold = 1000;

        await using var sqlConn = new SqlConnection(sqlConnStr);
        await sqlConn.OpenAsync();

        int rowsCount = 0;
        try
        {
            var filePath = Path.Combine(baseFolder, $"{table.Schema}_{table.TableName}.txt");

            var exportableColumns = table.Columns.Where(a => !a.IsComputed).OrderBy(a => a.OrderNo).ToList();

            var colNames = string.Join(", ", exportableColumns.Select(c => $"[{c.Name}]"));
            var sql = $"SELECT {colNames} FROM [{table.Schema}].[{table.TableName}]";
            await using var cmd = new SqlCommand(sql, sqlConn);
            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess);

            await using var writer = new StreamWriter(filePath, false, Encoding.UTF8, bufferSize: 64 * 1024);
            var buffer = new StringBuilder();
            var row = new StringBuilder();

            while (await reader.ReadAsync())
            {
                row.Clear();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    if (reader.IsDBNull(i))
                    {
                        row.Append("\\N");
                    }
                    else
                    {
                        var val = reader.GetValue(i);
                        switch (exportableColumns[i].SqlServerType)
                        {
                            case "bit":
                                row.Append($"{val.ToString()!.ToLower()}");
                                break;
                            case "date":
                                row.Append($"{((DateTime)val).ToString("yyyy-MM-dd")}");
                                break;
                            case "datetime2":
                                row.Append($"{((DateTime)val).ToString("yyyy-MM-ddTHH:mm:ss.fffffff")}");
                                break;
                            case "datetimeoffset":
                                row.Append($"{((DateTimeOffset)val).ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz")}");
                                break;
                            case "int" or "bigint" or "smallint":
                                row.Append($"{val}");
                                break;
                            case "double":
                                row.Append($"{((double)val).ToString(CultureInfo.InvariantCulture)}");
                                break;
                            case "float":
                                row.Append($"{((float)val).ToString(CultureInfo.InvariantCulture)}");
                                break;
                            case "decimal":
                                row.Append($"{((decimal)val).ToString(CultureInfo.InvariantCulture)}");
                                break;
                            case "varbinary":
                                row.Append($"\\\\x{Convert.ToHexString((byte[])val)}");
                                break;
                            default:
                                row.Append($"{EscapeForPostgresCopyText(val.ToString())}");
                                break;
                        }
                    }

                    if (i < reader.FieldCount - 1)
                    {
                        row.Append(',');
                    }
                }

                buffer.AppendLine(row.ToString());
                rowsCount++;

                if (rowsCount % flushThreshold == 0)
                {
                    await writer.WriteAsync(buffer.ToString());
                    buffer.Clear();
                }
            }

            // Write any remaining rows
            if (buffer.Length > 0)
                await writer.WriteAsync(buffer.ToString());

            Log.Information($"Exported {table.Schema}.{table.TableName} - {rowsCount} rows");
        }
        catch (Exception ex)
        {
            Log.Error(ex, $"Can't export data from {table.Schema}.{table.TableName} - line {rowsCount + 1}.");
            throw;
        }
    }

    static string EscapeForPostgresCopyText(string input)
    {
        if (input == null) return "\\N"; // PostgreSQL null representation

        var sb = new StringBuilder(input.Length + 16);
        foreach (var ch in input)
        {
            switch (ch)
            {
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                case '\v': sb.Append("\\v"); break;
                case '\\': sb.Append("\\\\"); break;
                case '"': sb.Append("\""); break;
                case ',': sb.Append("\\054"); break;
                case '\0': sb.Append(""); break; // Postgres not allowed
                default:
                    if (ch < 0x20 || ch == 0x7F)
                        sb.Append($"\\{(int)ch:000}"); // octal for non-printables
                    else
                        sb.Append(ch);
                    break;
            }
        }
        return sb.ToString();
    }


    static async Task ImportCsvToPostgres(string pgConnStr, TableSchema table, string baseFolder, bool dbo2Public = false)
    {
        var filePath = Path.Combine(baseFolder, $"{table.Schema}_{table.TableName}.txt");

        await using var conn = new NpgsqlConnection(pgConnStr);
        await conn.OpenAsync();

        var schema = table.Schema;
        if (dbo2Public && schema.Equals("dbo", StringComparison.InvariantCultureIgnoreCase)) { schema = "public"; }

        //await using var writer = conn.BeginTextImport($"COPY \"{table.Schema}\".\"{table.TableName}\" FROM STDIN WITH (FORMAT TEXT, HEADER FALSE, DELIMITER ',', QUOTE '''', NULL '\\N')");
        await using var writer = conn.BeginTextImport($"COPY \"{schema}\".\"{table.TableName}\" FROM STDIN WITH (FORMAT TEXT, HEADER FALSE, DELIMITER ',')");

        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1024 * 64);
        using var reader = new StreamReader(fileStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1024 * 64);

        char[] buffer = new char[1024 * 32];
        int read;
        while ((read = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
        {
            await writer.WriteAsync(buffer, 0, read);
        }

        Log.Information($"Imported into {table.Schema}.{table.TableName}");
    }

    static async Task ApplySqlBatch(string pgConnStr, List<string> statements)
    {
        await using var conn = new NpgsqlConnection(pgConnStr);

        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand()
        {
            Connection = conn,
            CommandTimeout = 600
        };

        foreach (var sql in statements)
        {
            cmd.CommandText = sql;

            try
            {
                await cmd.ExecuteNonQueryAsync();
                Log.Information($"Executed: {sql}");
            }
            catch (Exception ex)
            {
                Log.Error($"❌ Error: {sql} — {ex.Message}");
            }
        }
    }

    static string ConvertComputed(string schema, string table, string column, string definition, Dictionary<string, string> mapping)
    {
        if (mapping.TryGetValue($"{schema}.{table}.{column}", out var pgDefinition))
        {
            return pgDefinition;
        }

        string convertedDefinition = Regex.Replace(definition, @"\[(.*?)\]", "\"$1\"")
            .Replace("+", "||")
            .Replace("replicate(", "repeat(", StringComparison.InvariantCultureIgnoreCase)
            .Replace("len(", "length(", StringComparison.InvariantCultureIgnoreCase)
            ;

        return convertedDefinition;
    }

    static string TranslateSqlServerExpressionToPg(string expr, string schema, string table, Dictionary<string, HashSet<string>> boolColumns, out bool hasUnmapped)
    {
        hasUnmapped = false;
        if (string.IsNullOrWhiteSpace(expr)) return expr;

        string key = $"{schema}.{table}";

        var knownFuncs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["ISNULL"] = "COALESCE",
            ["GETDATE"] = "CURRENT_TIMESTAMP",
            ["SYSDATETIME"] = "CURRENT_TIMESTAMP",
            ["NEWID"] = "gen_random_uuid",
            ["LEN"] = "LENGTH"
        };

        var translated = TrimBalancedParentheses(expr);

        // Replace known SQL Server functions with PostgreSQL equivalents
        foreach (var kvp in knownFuncs)
            translated = Regex.Replace(translated, $@"\b{Regex.Escape(kvp.Key)}\(", kvp.Value + "(", RegexOptions.IgnoreCase);

        // Replace [Column] with "Column"
        translated = Regex.Replace(translated, @"\[(.*?)\]", "\"$1\"");

        // Replace bit-type column comparisons (only if column is known to be boolean)
        if (boolColumns.TryGetValue(key, out var colSet))
        {
            foreach (var col in colSet)
            {
                translated = Regex.Replace(translated,
                    $@"\(?\[?""?{Regex.Escape(col)}""?\]?\)?\s*=\s*\(?\s*1\s*\)?",
                    $"\"{col}\" = TRUE",
                    RegexOptions.IgnoreCase);

                translated = Regex.Replace(translated,
                    $@"\(?\[?""?{Regex.Escape(col)}""?\]?\)?\s*=\s*\(?\s*0\s*\)?",
                    $"\"{col}\" = FALSE",
                    RegexOptions.IgnoreCase);
            }
        }

        var functions = Regex.Matches(expr, @"\b(\w+)\(")
            .Cast<Match>()
            .Select(m => m.Groups[1].Value.ToUpperInvariant())
            .Distinct();

        foreach (var func in functions)
        {
            if (!knownFuncs.ContainsKey(func))
            {
                hasUnmapped = true;
                Log.Warning($"⚠️  Warning: Unmapped function in index filter: {func} (in expression: {expr})");
            }
        }

        return translated;
    }
}
