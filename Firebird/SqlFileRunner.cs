using FirebirdSql.Data.FirebirdClient;
using System.Text;
using System.Text.RegularExpressions;

namespace SenteApp.Firebird
{
        public sealed class SqlFileRunner
        {
            public void ExecuteFolder(FbConnection con, FbTransaction tx, string folderPath)
            {
                if (!Directory.Exists(folderPath))
                {
                    Console.WriteLine($"[SKIP] Folder not found: {folderPath}");
                    return;
                }

                var files = Directory.GetFiles(folderPath, "*.sql")
                    .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
                    .ToArray();

                if (files.Length == 0)
                {
                    Console.WriteLine($"[SKIP] No .sql files in: {folderPath}");
                    return;
                }

                var kind = Path.GetFileName(folderPath).ToLowerInvariant();
                Console.WriteLine($"[RUN] {kind} ({files.Length} files)");

                string? currentFile = null;

                try
                {
                    foreach (var file in files)
                    {
                        currentFile = file;
                        var objectName = Path.GetFileNameWithoutExtension(file);

                        // Idempotency rules
                        if (kind == "domains" && DomainExists(con, tx, objectName))
                        {
                            Console.WriteLine($"  [SKIP] {objectName} (domain exists)");
                            continue;
                        }

                        if (kind == "tables")
                        {
                            ApplyTableScript(con, tx, file, objectName);
                            continue;
                        }

                    // Procedures: CREATE OR ALTER - safe to run always
                    var sql = File.ReadAllText(file, Encoding.UTF8).Trim();
                        if (string.IsNullOrWhiteSpace(sql))
                        {
                            Console.WriteLine($"  [SKIP] {Path.GetFileName(file)} (empty)");
                            continue;
                        }

                        using var cmd = new FbCommand(sql, con, tx);
                        cmd.ExecuteNonQuery();

                        Console.WriteLine($"  [OK] {Path.GetFileName(file)}");
                    }

                }
                catch (Exception ex)
                {

                    throw new InvalidOperationException(
                        $"Failed executing scripts in folder '{folderPath}'. Transaction rolled back. " +
                        $"File: {currentFile ?? "<unknown>"}. Error: {ex.Message}",
                        ex);
                }
            }

            private static bool DomainExists(FbConnection con, FbTransaction tx, string domainName)
            {
                const string sql = @"
                    SELECT 1
                    FROM rdb$fields f
                    WHERE f.rdb$system_flag = 0
                      AND TRIM(f.rdb$field_name) = @name";

                using var cmd = new FbCommand(sql, con, tx);
                cmd.Parameters.AddWithValue("@name", domainName);
                var result = cmd.ExecuteScalar();
                return result is not null && result is not DBNull;
            }

            private static bool TableExists(FbConnection con, FbTransaction tx, string tableName)
            {
                const string sql = @"
                    SELECT 1
                    FROM rdb$relations r
                    WHERE r.rdb$system_flag = 0
                      AND r.rdb$view_blr IS NULL
                      AND TRIM(r.rdb$relation_name) = @name";

                using var cmd = new FbCommand(sql, con, tx);
                cmd.Parameters.AddWithValue("@name", tableName);
                var result = cmd.ExecuteScalar();
                return result is not null && result is not DBNull;
            }

        public void ExecuteProceduresWithDependencies(FbConnection con, FbTransaction tx, string proceduresDir)
        {

            var files = Directory.GetFiles(proceduresDir, "*.sql")
            .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
            .ToArray();

            if (files.Length == 0)
            {
                Console.WriteLine($"[SKIP] No .sql files in: {proceduresDir}");
                return;
            }

            // Known procedure names = file names without extension
            var procNames = new HashSet<string>(
                files.Select(f => Path.GetFileNameWithoutExtension(f)),
                StringComparer.OrdinalIgnoreCase);

            // Build dependency graph using EXECUTE PROCEDURE 
            var deps = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var file in files)
            {
                var proc = Path.GetFileNameWithoutExtension(file);
                var sql = File.ReadAllText(file);

                var called = ExtractCalledProcedures(sql, procNames);
                called.Remove(proc);

                deps[proc] = called;
            }

            var ordered = TopologicalSort(procNames, deps);

            Console.WriteLine($"[RUN] procedures (dependency order + retry) ({ordered.Count} files)");

            // Retry rounds: try to execute what we can, postpone failures
            var remaining = new List<string>(ordered);
            var round = 1;

            while (remaining.Count > 0)
            {
                Console.WriteLine($"  [ROUND {round}] remaining: {remaining.Count}");

                var nextRemaining = new List<string>();
                var successesThisRound = 0;
                var lastErrorByProc = new Dictionary<string, Exception>(StringComparer.OrdinalIgnoreCase);

                foreach (var proc in remaining)
                {
                    var file = Path.Combine(proceduresDir, $"{proc}.sql");

                    try
                    {
                        ExecuteSqlFile(con, tx, file);
                        Console.WriteLine($"    [OK] {Path.GetFileName(file)}");
                        successesThisRound++;
                    }
                    catch (Exception ex)
                    {
                        // Keep it for next round; dependency might not exist yet
                        nextRemaining.Add(proc);
                        lastErrorByProc[proc] = ex;
                        Console.WriteLine($"    [WAIT] {Path.GetFileName(file)} -> {ex.Message}");
                    }
                }

                if (successesThisRound == 0)
                {
                    // Nothing progressed => cycle or unresolved refs
                    var msg = "Cannot resolve procedure dependencies. " +
                                "No progress in retry round. Remaining procedures: " +
                                string.Join(", ", nextRemaining);

                    throw new InvalidOperationException(msg, lastErrorByProc.Values.FirstOrDefault());
                }

                remaining = nextRemaining;
                round++;
            }

            if (!Directory.Exists(proceduresDir))
            {
                Console.WriteLine($"[SKIP] Folder not found: {proceduresDir}");
                return;
            }            
        }

        private static HashSet<string> ExtractCalledProcedures(string sql, HashSet<string> knownProcNames)
        {
            // Minimal: EXECUTE PROCEDURE <name>
            // Captures names like ABC, ABC_1, "My Proc"
            var rx = new Regex(@"\bEXECUTE\s+PROCEDURE\s+(""[^""]+""|[A-Z0-9_]+)\b",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (Match m in rx.Matches(sql))
            {
                var raw = m.Groups[1].Value.Trim();

                // Strip quotes if present
                var name = raw.StartsWith("\"") && raw.EndsWith("\"")
                    ? raw.Substring(1, raw.Length - 2).Replace("\"\"", "\"")
                    : raw;

                if (knownProcNames.Contains(name))
                    result.Add(name);
            }

            return result;
        }

        private static List<string> TopologicalSort(HashSet<string> nodes, Dictionary<string, HashSet<string>> deps)
        {
            // Kahn’s algorithm
            var incomingCount = nodes.ToDictionary(n => n, _ => 0, StringComparer.OrdinalIgnoreCase);

            foreach (var (n, dset) in deps)
            {
                foreach (var d in dset)
                {
                    if (incomingCount.ContainsKey(n))
                        incomingCount[n]++;
                }
            }

            var queue = new Queue<string>(incomingCount.Where(kv => kv.Value == 0).Select(kv => kv.Key));
            var ordered = new List<string>();

            // reverse lookup
            var reverse = nodes.ToDictionary(n => n, _ => new List<string>(), StringComparer.OrdinalIgnoreCase);
            foreach (var (n, dset) in deps)
            {
                foreach (var d in dset)
                    reverse[d].Add(n);
            }

            while (queue.Count > 0)
            {
                var n = queue.Dequeue();
                ordered.Add(n);

                foreach (var dependent in reverse[n])
                {
                    incomingCount[dependent]--;
                    if (incomingCount[dependent] == 0)
                        queue.Enqueue(dependent);
                }
            }

            if (ordered.Count != nodes.Count)
            {
                // Cycle or unresolved deps. Minimal safe fallback:
                // execute remaining in deterministic order after those we could sort.
                var remaining = nodes.Except(ordered, StringComparer.OrdinalIgnoreCase)
                    .OrderBy(x => x, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                ordered.AddRange(remaining);
            }

            return ordered;
        }

        private void ExecuteSqlFile(FbConnection con, FbTransaction tx, string file)
        {
            var sql = File.ReadAllText(file).Trim();
            if (string.IsNullOrWhiteSpace(sql)) return;

            using var cmd = new FbCommand(sql, con, tx);
            cmd.ExecuteNonQuery();
        }

        private void ApplyTableScript(FbConnection con, FbTransaction tx, string filePath, string tableName)
        {
            var sql = File.ReadAllText(filePath, Encoding.UTF8);

            // If table doesn't exist -> just run CREATE TABLE script
            if (!TableExists(con, tx, tableName))
            {
                ExecuteSqlText(con, tx, sql);
                Console.WriteLine($"  [OK] {Path.GetFileName(filePath)} (created)");
                return;
            }

            // Table exists -> add missing columns
            var def = TableScriptParser.ParseCreateTable(sql);

            var existingCols = GetExistingTableColumns(con, tx, tableName);

            var added = 0;
            foreach (var col in def.Columns)
            {
                if (existingCols.Contains(col.Name))
                    continue;

                var alter = $"ALTER TABLE {EscapeIdent(tableName)} ADD {EscapeIdent(col.Name)} {col.TypeSql}{(col.NotNull ? " NOT NULL" : "")};";
                ExecuteSqlText(con, tx, alter);

                Console.WriteLine($"  [OK] ALTER TABLE {tableName} ADD {col.Name}");
                added++;
            }

            if (added == 0)
                Console.WriteLine($"  [OK] {Path.GetFileName(filePath)} (no changes)");
        }

        private static HashSet<string> GetExistingTableColumns(FbConnection con, FbTransaction tx, string tableName)
        {
            const string sql = @"
                SELECT TRIM(rf.rdb$field_name) AS col_name
                FROM rdb$relation_fields rf
                WHERE TRIM(rf.rdb$relation_name) = @tableName
                ORDER BY rf.rdb$field_position";

            using var cmd = new FbCommand(sql, con, tx);
            cmd.Parameters.AddWithValue("@tableName", tableName);

            using var r = cmd.ExecuteReader();

            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (r.Read())
                set.Add(r.GetString(0).Trim());

            return set;
        }

        private void ExecuteSqlText(FbConnection con, FbTransaction tx, string sql)
        {
            sql = sql.Trim();
            if (string.IsNullOrWhiteSpace(sql)) return;

            using var cmd = new FbCommand(sql, con, tx);
            cmd.ExecuteNonQuery();
        }

        private static string EscapeIdent(string ident)
        {
            if (!string.IsNullOrWhiteSpace(ident)
                && char.IsLetter(ident[0])
                && ident.All(ch => char.IsLetterOrDigit(ch) || ch == '_'))
                return ident;

            return "\"" + ident.Replace("\"", "\"\"") + "\"";
        }
    }
}

