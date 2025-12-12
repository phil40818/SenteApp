using SenteApp.Cli;
using SenteApp.Firebird;

namespace DbMetaTool
{
    public static class Program
    {
        // Przykładowe wywołania:
        // DbMetaTool build-db --connection-string "..." --db-dir "C:\db\fb5" --scripts-dir "C:\scripts"
        // DbMetaTool export-scripts --connection-string "..." --output-dir "C:\out"
        // DbMetaTool update-db --connection-string "..." --scripts-dir "C:\scripts"

        public static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Użycie:");
                Console.WriteLine("  build-db --connection-string <connStr> --db-dir <ścieżka> --scripts-dir <ścieżka>");
                Console.WriteLine("  export-scripts --connection-string <connStr> --output-dir <ścieżka>");
                Console.WriteLine("  update-db --connection-string <connStr> --scripts-dir <ścieżka>");
                return 1;
            }

            try
            {
                var command = args[0].ToLowerInvariant();

                switch (command)
                {
                    case "export-scripts":
                        {
                            var connStr = Arguments.GetRequired(args, "--connection-string");
                            var outputDir = Arguments.GetRequired(args, "--output-dir");

                            var exporter = new FirebirdMetadataExporter();
                            exporter.ExportAll(connStr, outputDir);

                            Console.WriteLine("Skrypty zostały wyeksportowane pomyślnie.");
                            return 0;
                        }

                    case "build-db":
                        {
                            var connStr = Arguments.GetRequired(args, "--connection-string");
                            var dbDir = Arguments.GetRequired(args, "--db-dir");
                            var scriptsDir = Arguments.GetRequired(args, "--scripts-dir");

                            var builder = new FirebirdDatabaseBuilder();
                            var dbPath = builder.BuildDatabase(connStr, dbDir, scriptsDir);

                            Console.WriteLine($"Baza zbudowana: {dbPath}");
                            return 0;
                        }

                    case "update-db":
                        {
                            var connStr = Arguments.GetRequired(args, "--connection-string");
                            var scriptsDir = Arguments.GetRequired(args, "--scripts-dir");

                            var updater = new FirebirdDatabaseUpdater();
                            updater.UpdateDatabase(connStr, scriptsDir);

                            Console.WriteLine("Baza danych została zaktualizowana pomyślnie.");
                            return 0;
                        }

                    default:
                        Console.WriteLine($"Nieznane polecenie: {command}");
                        return 1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return -1;
            }
        }
    }
}
