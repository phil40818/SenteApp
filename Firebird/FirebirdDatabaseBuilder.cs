using FirebirdSql.Data.FirebirdClient;

namespace SenteApp.Firebird
{
    public sealed class FirebirdDatabaseBuilder
    {
        /// <summary>
        /// Buduje nową bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public string BuildDatabase(string serverConnectionString, string databaseDirectory, string scriptsDirectory)
        {
            // TODO:
            // 1) Utwórz pustą bazę danych FB 5.0 w katalogu databaseDirectory.
            // 2) Wczytaj i wykonaj kolejno skrypty z katalogu scriptsDirectory
            //    (tylko domeny, tabele, procedury).
            // 3) Obsłuż błędy i wyświetl raport.

            if (string.IsNullOrWhiteSpace(serverConnectionString))
                throw new ArgumentException("serverConnectionString is required.", nameof(serverConnectionString));
            if (string.IsNullOrWhiteSpace(databaseDirectory))
                throw new ArgumentException("databaseDirectory is required.", nameof(databaseDirectory));
            if (string.IsNullOrWhiteSpace(scriptsDirectory))
                throw new ArgumentException("scriptsDirectory is required.", nameof(scriptsDirectory));

            Directory.CreateDirectory(databaseDirectory);

            var dbPath = Path.Combine(databaseDirectory, "generated2.fdb");
            if (File.Exists(dbPath))
                File.Delete(dbPath);

            var csb = new FbConnectionStringBuilder(serverConnectionString)
            {
                Database = dbPath,
            };

            // Creates physical db file .fdb
            // signature: CreateDatabase(connectionString, pageSize, forcedWrites, overwrite) :contentReference[oaicite:1]{index=1}
            FbConnection.CreateDatabase(csb.ToString(), 4096, true, overwrite: true);

            using var con = new FbConnection(csb.ToString());
            con.Open();

            Console.WriteLine("Created DB: " + dbPath);

            var runner = new SqlFileRunner();

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

            return dbPath;
        }
    }
}