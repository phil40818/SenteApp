using FirebirdSql.Data.FirebirdClient;

namespace SenteApp.Firebird
{
    public sealed class FirebirdDatabaseUpdater
    {
        /// <summary>
        /// Aktualizuje istniejącą bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public void UpdateDatabase(string connectionString, string scriptsDirectory)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Wykonaj skrypty z katalogu scriptsDirectory (tylko obsługiwane elementy).
            // 3) Zadbaj o poprawną kolejność i bezpieczeństwo zmian.

            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("connectionString is required.", nameof(connectionString));
            if (string.IsNullOrWhiteSpace(scriptsDirectory))
                throw new ArgumentException("scriptsDirectory is required.", nameof(scriptsDirectory));

            if (!Directory.Exists(scriptsDirectory))
                throw new DirectoryNotFoundException($"Scripts directory not found: {scriptsDirectory}");

            using var con = new FbConnection(connectionString);
            con.Open();

            LogConnectionInfo(con, scriptsDirectory);

            var runner = new SqlFileRunner();

            // Safe order
            using var tx = con.BeginTransaction();
            try
            {
                runner.ExecuteFolder(con, tx, Path.Combine(scriptsDirectory, "domains"));
                runner.ExecuteFolder(con, tx, Path.Combine(scriptsDirectory, "tables"));
                runner.ExecuteProceduresWithDependencies(con, tx, Path.Combine(scriptsDirectory, "procedures"));

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        private static void LogConnectionInfo(FbConnection con, string scriptsDirectory)
        {
            using var cmd = new FbCommand("SELECT rdb$get_context('SYSTEM','DB_NAME') FROM rdb$database", con);
            var dbName = (string?)cmd.ExecuteScalar();

            Console.WriteLine($"Target DB   : {dbName}");
            Console.WriteLine($"Scripts dir : {Path.GetFullPath(scriptsDirectory)}");
        }
    }
}
