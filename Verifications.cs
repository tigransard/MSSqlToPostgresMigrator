using Microsoft.Data.SqlClient;
using Npgsql;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DbMigrator;

partial class Program
{
    private static async Task<bool> RunVerifyTablesMode(AppConfig config, string sqlConnStr, string pgConnStr, string baseFolder)
    {
        var tables = await ExtractSqlServerSchema(sqlConnStr, false);
        var rowCounts = new Dictionary<string, (long Source, long Dest)>();

        await using (var srcConn = new SqlConnection(sqlConnStr))
        {
            await srcConn.OpenAsync();

            await using (var destConn = new NpgsqlConnection(pgConnStr))
            {
                await destConn.OpenAsync();

                foreach (var table in tables)
                {
                    string fullName = $"{table.Schema}.{table.TableName}";
                    var (sourceCount, _, _) = await GetTableStatsSqlServer(srcConn, table, false);
                    long destCount = await GetRowCountInPostgres(destConn, table);
                    rowCounts[fullName] = (sourceCount, destCount);
                }
            }
        }

        bool issueEncountered = false;
        var reportLines = new List<string>();
        reportLines.Add($"Counts verification report - {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        reportLines.Add("------------------------------------------------------");

        foreach (var (tableName, (src, dst)) in rowCounts)
        {
            string status = src == dst ? "✅ MATCH" : "❌ MISMATCH";
            if (src == dst)
            {
                Log.Information($"✅ {tableName}: {src} rows");
            }
            else
            {
                issueEncountered = true;
                Log.Error($"❌ {tableName}: source={src}, destination={dst}");
            }

            reportLines.Add($"{tableName,-80} | source: {src,-10} | destination: {dst,-10} | {status}");
        }

        await File.WriteAllLinesAsync(Path.Combine(baseFolder, "_rows_count_report.log"), reportLines);
        Log.Information("Written row count verification to _rows_count_report.log");
        return issueEncountered;
    }

    public static void ReadDataRecords(IDbCommand dbCommand, Dictionary<string, DataRecord> output)
    {
        dbCommand.CommandTimeout = 600;
        using var reader = dbCommand.ExecuteReader();

        var columnsCount = reader.FieldCount;

        while (reader.Read())
        {
            var record = new DataRecord() { Values = new Dictionary<string, object>() };

            for (int i = 0; i < columnsCount; i++)
            {
                var columnName = reader.GetName(i);
                if (reader.IsDBNull(i))
                {
                    record.Values[columnName] = null;
                }
                else
                {
                    var value = reader.GetValue(i);
                    if (value is decimal valueDecimal)
                    {
                        value = Math.Round(valueDecimal, 2);
                    }

                    record.Values[columnName] = value;
                }
            }

            output.Add(reader["id"].ToString(), record);
        }

        reader.Close();
    }

    private static async Task<bool> RunVerifyDataMode(string sqlConnStr, string pgConnStr, string baseFolder)
    {
        var queriesToCheck = DbVerificationQueries();

        await using var srcConn = new SqlConnection(sqlConnStr);
        await srcConn.OpenAsync();

        await using var destConn = new NpgsqlConnection(pgConnStr);
        await destConn.OpenAsync();

        bool issueEncountered = false;
        foreach (var query in queriesToCheck)
        {
            var srcData = new Dictionary<string, DataRecord>();
            var destData = new Dictionary<string, DataRecord>();

            await using var sqlCommand = new SqlCommand(query.Value, srcConn);
            await using var pgCommand = new NpgsqlCommand(query.Value, destConn);

            ReadDataRecords(sqlCommand, srcData);
            ReadDataRecords(pgCommand, destData);

            var reportLines = new List<string>
            {
                $"Report - {query.Key} - lines count => {srcData.Count} / {destData.Count}",
                "------------------------------------------------------"
            };

            Log.Information($"Report - {query.Key} - lines count => {srcData.Count} / {destData.Count}");

            if (srcData.Count != destData.Count)
            {
                issueEncountered = true;
                reportLines.Add(
                    $"returned amount of rows doesn't match for {query.Key} - source = {srcData.Count}, destination = {destData.Count}");
                Log.Error(
                    $"returned amount of rows doesn't match for {query.Key} - source = {srcData.Count}, destination = {destData.Count}");
            }
            else
            {
                foreach (var item in srcData)
                {
                    if (destData.TryGetValue(item.Key, out var destItem))
                    {
                        if (destItem.ToString() != item.Value.ToString())
                        {
                            issueEncountered = true;
                            reportLines.Add($"data for ID {item.Key} doesn't match {item.Value} <=> {destItem}");
                            Log.Error($"data for ID {item.Key} doesn't match {item.Value} <=> {destItem}");
                        }
                    }
                    else
                    {
                        reportLines.Add($"no matching data for ID {item.Key}");
                        Log.Error($"no matching data for ID {item.Key}");
                    }
                }
            }

            reportLines.Add("");

            await File.AppendAllLinesAsync(Path.Combine(baseFolder, "_data_consistency_report.log"), reportLines);
        }
        return issueEncountered;
    }

    private static IReadOnlyDictionary<string, string> DbVerificationQueries()
    {
        return new Dictionary<string, string>
        {
            /*
             * In every query there should always be a unique column with name "id"
            {
                "SampleDataCheckQuery",
                @"select ""Id"" id, ""Column1"" num1, ""Column2"" num2
                  from schema.""TableName"" "
            },
            */
        };
    }

    internal record DataRecord
    {
        public Dictionary<string, object> Values { get; set; }

        public override string ToString()
        {
            return string.Join(", ", Values.Select(kv => $"{kv.Key}: {kv.Value}"));
        }
    }
}