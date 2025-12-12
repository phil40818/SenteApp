using FirebirdSql.Data.FirebirdClient;
using System.Text;

namespace SenteApp.Firebird
{
    public sealed class FirebirdMetadataExporter
    {
        /// <summary>
        /// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
        /// </summary>
        public void ExportAll(string connectionString, string outputRoot)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Pobierz metadane domen, tabel (z kolumnami) i procedur.
            // 3) Wygeneruj pliki .sql / .json / .txt w outputDirectory.

            ValidateNotEmpty(connectionString, nameof(connectionString));
            ValidateAndCreateDir(outputRoot, nameof(outputRoot));

            var dirs = PrepareExportDirectories(outputRoot);

            using var con = new FbConnection(connectionString);
            con.Open();

            LogConnectionInfo(con, outputRoot);

            ExportDomains(con, dirs.DomainsDir);
            ExportTables(con, dirs.TablesDir);
            ExportProcedures(con, dirs.ProceduresDir); 
        }

        private static void ExportProcedures(FbConnection con, string outDir)
        {
            Directory.CreateDirectory(outDir);

            const string procsSql = @"
                SELECT
                  TRIM(p.rdb$procedure_name) AS name,
                  p.rdb$procedure_source     AS source
                FROM rdb$procedures p
                WHERE p.rdb$system_flag = 0
                ORDER BY 1;";

            using var pcmd = new FbCommand(procsSql, con);
            using var pr = pcmd.ExecuteReader();

            while (pr.Read())
            {
                var procName = pr.GetString(0).Trim();

                // In some cases source might be NULL (depending on how it was created)
                var source = pr["source"] is DBNull ? "" : Convert.ToString(pr["source"]) ?? "";
                source = source.Trim();

                var (inputs, outputs) = LoadProcedureParameters(con, procName);

                // Build SQL type for params: prefer user domains when possible
                string BuildParamSql(ProcParam p)
                {
                    var typeSql = IsUserDomain(con, p.FieldSource)
                        ? EscapeIdent(p.FieldSource)
                        : FirebirdTypeToSql(p.FieldType, p.Length, p.Precision, p.Scale);

                    return $"{EscapeIdent(p.Name)} {typeSql}";
                }

                var sb = new StringBuilder();

                sb.AppendLine($"CREATE OR ALTER PROCEDURE {EscapeIdent(procName)}");

                if (inputs.Count > 0)
                {
                    sb.AppendLine("(");
                    for (int i = 0; i < inputs.Count; i++)
                    {
                        var line = "  " + BuildParamSql(inputs[i]);
                        sb.AppendLine(i < inputs.Count - 1 ? line + "," : line);
                    }
                    sb.AppendLine(")");
                }

                if (outputs.Count > 0)
                {
                    sb.AppendLine("RETURNS (");
                    for (int i = 0; i < outputs.Count; i++)
                    {
                        var line = "  " + BuildParamSql(outputs[i]);
                        sb.AppendLine(i < outputs.Count - 1 ? line + "," : line);
                    }
                    sb.AppendLine(")");
                }

                sb.AppendLine("AS");

                // If DB stored full DDL (rare), keep it; otherwise treat as body.
                if (!string.IsNullOrWhiteSpace(source) &&
                    source.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase))
                {
                    File.WriteAllText(Path.Combine(outDir, $"{procName}.sql"), source + Environment.NewLine, Encoding.UTF8);
                    continue;
                }

                // If source is empty, emit minimal body
                if (string.IsNullOrWhiteSpace(source))
                {
                    sb.AppendLine("BEGIN");
                    sb.AppendLine("  -- TODO: procedure source not available");
                    sb.AppendLine("END");
                }
                else
                {
                    sb.AppendLine(source);
                }

                sb.AppendLine();
                File.WriteAllText(Path.Combine(outDir, $"{procName}.sql"), sb.ToString(), Encoding.UTF8);
            }
        }

        private static void ExportDomains(FbConnection con, string outDir)
        {
            const string sql = @"
                SELECT TRIM(f.rdb$field_name) AS name,
                       f.rdb$field_type AS field_type,
                       f.rdb$field_length AS field_length
                FROM rdb$fields f
                WHERE f.rdb$system_flag = 0
                ORDER BY 1;";

            using var cmd = new FbCommand(sql, con);
            using var r = cmd.ExecuteReader();

            while (r.Read())
            {
                var name = r.GetString(0).Trim();

                // Skip internal/generated domains (RDB$1, RDB$2, ...)
                if (name.StartsWith("RDB$", StringComparison.OrdinalIgnoreCase))
                    continue;

                var fieldType = Convert.ToInt32(r["field_type"]);
                var len = r["field_length"] is DBNull ? 0 : Convert.ToInt32(r["field_length"]);

                var typeSql = fieldType switch
                {
                    37 => $"VARCHAR({len})",
                    14 => $"CHAR({len})",
                    8 => "INTEGER",
                    _ => "BLOB"
                };

                var script = $"CREATE DOMAIN {EscapeIdent(name)} AS {typeSql};{Environment.NewLine}";
                File.WriteAllText(Path.Combine(outDir, $"{name}.sql"), script, Encoding.UTF8);
            }
        }

        private static void ExportTables(FbConnection con, string outDir)
        {
            const string tablesSql = @"
                SELECT TRIM(r.rdb$relation_name) AS table_name
                FROM rdb$relations r
                WHERE r.rdb$system_flag = 0
                  AND r.rdb$view_blr IS NULL
                ORDER BY 1;";

            const string colsSql = @"
                SELECT
                  TRIM(rf.rdb$field_name)   AS column_name,
                  TRIM(rf.rdb$field_source) AS field_source,
                  rf.rdb$null_flag          AS null_flag,
                  rf.rdb$field_position     AS pos,
                  f.rdb$field_type          AS field_type,
                  f.rdb$field_length        AS field_length,
                  f.rdb$field_precision     AS field_precision,
                  f.rdb$field_scale         AS field_scale
                FROM rdb$relation_fields rf
                JOIN rdb$fields f ON f.rdb$field_name = rf.rdb$field_source
                WHERE rf.rdb$relation_name = @tableName
                ORDER BY rf.rdb$field_position";

            using var tcmd = new FbCommand(tablesSql, con);
            using var tr = tcmd.ExecuteReader();

            while (tr.Read())
            {
                var tableName = tr.GetString(0).Trim();

                if (tableName.StartsWith("RDB$", StringComparison.OrdinalIgnoreCase))
                    continue;

                var sb = new StringBuilder();
                sb.AppendLine($"CREATE TABLE {EscapeIdent(tableName)} (");

                using var ccmd = new FbCommand(colsSql, con);
                ccmd.Parameters.AddWithValue("@tableName", tableName);

                using var cr = ccmd.ExecuteReader();

                var first = true;
                while (cr.Read())
                {
                    var colName = cr.GetString(0).Trim();
                    var fieldSource = cr.GetString(1).Trim();
                    var nullFlag = cr["null_flag"] is DBNull ? (short?)null : Convert.ToInt16(cr["null_flag"]);

                    string colTypeSql;
                    if (IsUserDomain(con, fieldSource))
                    {
                        colTypeSql = EscapeIdent(fieldSource);
                    }
                    else
                    {
                        var fieldType = Convert.ToInt32(cr["field_type"]);
                        var length = cr["field_length"] is DBNull ? 0 : Convert.ToInt32(cr["field_length"]);
                        var precision = cr["field_precision"] is DBNull ? (int?)null : Convert.ToInt32(cr["field_precision"]);
                        var scale = cr["field_scale"] is DBNull ? (int?)null : Convert.ToInt32(cr["field_scale"]);

                        colTypeSql = FirebirdTypeToSql(fieldType, length, precision, scale);
                    }

                    var nullSql = nullFlag == 1 ? " NOT NULL" : "";

                    if (!first) sb.AppendLine(",");
                    sb.Append($"  {EscapeIdent(colName)} {colTypeSql}{nullSql}");
                    first = false;
                }

                sb.AppendLine();
                sb.AppendLine(");");

                File.WriteAllText(Path.Combine(outDir, $"{tableName}.sql"), sb.ToString(), Encoding.UTF8);
            }
        }

        private static (List<ProcParam> Inputs, List<ProcParam> Outputs) LoadProcedureParameters(FbConnection con, string procName)
        {
            // parameter_type: 0 = input, 1 = output
            const string sql = @"
            SELECT
              TRIM(pp.rdb$parameter_name)  AS param_name,
              pp.rdb$parameter_type        AS param_type,
              pp.rdb$parameter_number      AS param_number,
              TRIM(pp.rdb$field_source)    AS field_source,
              f.rdb$field_type             AS field_type,
              f.rdb$field_length           AS field_length,
              f.rdb$field_precision        AS field_precision,
              f.rdb$field_scale            AS field_scale
            FROM rdb$procedure_parameters pp
            JOIN rdb$fields f ON f.rdb$field_name = pp.rdb$field_source
            WHERE pp.rdb$procedure_name = @procName
            ORDER BY pp.rdb$parameter_type, pp.rdb$parameter_number";

            using var cmd = new FbCommand(sql, con);
            cmd.Parameters.AddWithValue("@procName", procName);

            using var r = cmd.ExecuteReader();

            var inputs = new List<ProcParam>();
            var outputs = new List<ProcParam>();

            while (r.Read())
            {
                var name = r.GetString(0).Trim();
                var type = Convert.ToInt32(r["param_type"]);
                var fieldSource = r.GetString(3).Trim();

                var p = new ProcParam(
                    Name: name,
                    FieldSource: fieldSource,
                    FieldType: Convert.ToInt32(r["field_type"]),
                    Length: r["field_length"] is DBNull ? 0 : Convert.ToInt32(r["field_length"]),
                    Precision: r["field_precision"] is DBNull ? (int?)null : Convert.ToInt32(r["field_precision"]),
                    Scale: r["field_scale"] is DBNull ? (int?)null : Convert.ToInt32(r["field_scale"])
                );

                if (type == 0) inputs.Add(p);
                else outputs.Add(p);
            }

            return (inputs, outputs);
        }

        private sealed record ProcParam(
            string Name,
            string FieldSource,
            int FieldType,
            int Length,
            int? Precision,
            int? Scale
        );

        private static bool IsUserDomain(FbConnection con, string domainName)
        {
            if (domainName.StartsWith("RDB$", StringComparison.OrdinalIgnoreCase))
                return false;

            const string sql = @"
                SELECT 1
                FROM rdb$fields f
                WHERE f.rdb$system_flag = 0
                  AND TRIM(f.rdb$field_name) = @name;";

            using var cmd = new FbCommand(sql, con);
            cmd.Parameters.AddWithValue("@name", domainName);
            var result = cmd.ExecuteScalar();
            return result is not null && result is not DBNull;
        }

        private static string FirebirdTypeToSql(int fieldType, int length, int? precision, int? scale)
        {
            // Minimal mapping
            return fieldType switch
            {
                7 => "SMALLINT",
                8 => "INTEGER",
                16 => precision is not null
                        ? $"NUMERIC({precision},{Math.Abs(scale ?? 0)})"
                        : "BIGINT",
                10 => "FLOAT",
                27 => "DOUBLE PRECISION",
                12 => "DATE",
                13 => "TIME",
                35 => "TIMESTAMP",
                37 => $"VARCHAR({(length > 0 ? length : 255)})",
                14 => $"CHAR({(length > 0 ? length : 1)})",
                261 => "BLOB",
                _ => "BLOB"
            };
        }

        private static string EscapeIdent(string ident)
        {
            if (!string.IsNullOrWhiteSpace(ident)
                && char.IsLetter(ident[0])
                && ident.All(ch => char.IsLetterOrDigit(ch) || ch == '_'))
                return ident;

            return "\"" + ident.Replace("\"", "\"\"") + "\"";
        }

        private static void ValidateNotEmpty(string value, string paramName)
        {
            if (string.IsNullOrWhiteSpace(value))
                throw new ArgumentException("Value cannot be empty.", paramName);
        }

        private static void ValidateAndCreateDir(string dir, string paramName)
        {
            if (string.IsNullOrWhiteSpace(dir))
                throw new ArgumentException("Directory path cannot be empty.", paramName);

            Directory.CreateDirectory(dir);
        }

        private static ExportDirs PrepareExportDirectories(string outputRoot)
        {
            var domains = Path.Combine(outputRoot, "domains");
            var tables = Path.Combine(outputRoot, "tables");
            var procs = Path.Combine(outputRoot, "procedures");

            Directory.CreateDirectory(domains);
            Directory.CreateDirectory(tables);
            Directory.CreateDirectory(procs);

            return new ExportDirs(domains, tables, procs);
        }

        private static void LogConnectionInfo(FbConnection con, string outputRoot)
        {
            using var cmd = new FbCommand("SELECT rdb$get_context('SYSTEM','DB_NAME') FROM rdb$database", con);
            var dbName = (string?)cmd.ExecuteScalar();

            Console.WriteLine($"Connected DB: {dbName}");
            Console.WriteLine($"Output dir  : {Path.GetFullPath(outputRoot)}");
        }

        private sealed record ExportDirs(string DomainsDir, string TablesDir, string ProceduresDir);
    }
}
