using FirebirdSql.Data.FirebirdClient;
using System.Text;

namespace SenteApp.Firebird
{
        public sealed class SqlFileRunner
        {
            public void ExecuteFolder(FbConnection con, string folderPath)
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

                using var tx = con.BeginTransaction();

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

                        if (kind == "tables" && TableExists(con, tx, objectName))
                        {
                            Console.WriteLine($"  [SKIP] {objectName} (table exists)");
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

                    tx.Commit();
                }
                catch (Exception ex)
                {
                    tx.Rollback();
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
        }
}

