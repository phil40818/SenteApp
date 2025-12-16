using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SenteApp.Firebird
{
    public static class TableScriptParser
    {
        public static TableDef ParseCreateTable(string sql)
        {
            var headerRx = new Regex(@"CREATE\s+TABLE\s+(""(?:[^""]|"""")*""|[A-Z0-9_]+)\s*\(",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

            var m = headerRx.Match(sql);
            if (!m.Success)
                throw new InvalidOperationException("Not a CREATE TABLE script in expected format.");

            var tableName = UnquoteIdent(m.Groups[1].Value);

            var start = m.Index + m.Length;
            var end = sql.LastIndexOf(");", StringComparison.OrdinalIgnoreCase);
            if (end < start) end = sql.LastIndexOf(")", StringComparison.OrdinalIgnoreCase);
            if (end < start)
                throw new InvalidOperationException("Could not find column list end in CREATE TABLE.");

            var inside = sql.Substring(start, end - start);

            var lines = inside
                .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(l => l.Trim().TrimEnd(','))
                .Where(l => !string.IsNullOrWhiteSpace(l))
                .ToList();

            var cols = new List<ColumnDef>();

            foreach (var line in lines)
            {
                // line: NAME DM_NAME
                // line: ID INTEGER NOT NULL
                var parts = SplitFirstToken(line);
                var colNameRaw = parts.Token;
                var rest = parts.Remainder.Trim();

                var notNull = rest.EndsWith("NOT NULL", StringComparison.OrdinalIgnoreCase);
                var typeSql = notNull ? rest[..^"NOT NULL".Length].Trim() : rest;

                cols.Add(new ColumnDef(UnquoteIdent(colNameRaw), typeSql, notNull));
            }

            return new TableDef(tableName, cols);
        }

        private static (string Token, string Remainder) SplitFirstToken(string s)
        {
            // Handles quoted identifiers: "My Col"
            if (s.StartsWith("\""))
            {
                var i = 1;
                while (i < s.Length)
                {
                    if (s[i] == '"')
                    {
                        if (i + 1 < s.Length && s[i + 1] == '"') { i += 2; continue; } // escaped quote
                        i++;
                        break;
                    }
                    i++;
                }

                var token = s.Substring(0, i);
                var rest = s.Substring(i);
                return (token.Trim(), rest);
            }

            var idx = s.IndexOf(' ');
            if (idx < 0) return (s.Trim(), "");
            return (s[..idx].Trim(), s[idx..].Trim());
        }

        private static string UnquoteIdent(string ident)
        {
            ident = ident.Trim();
            if (ident.StartsWith("\"") && ident.EndsWith("\""))
                return ident[1..^1].Replace("\"\"", "\"");

            return ident;
        }
    }

    public sealed record TableDef(string Name, List<ColumnDef> Columns);
    public sealed record ColumnDef(string Name, string TypeSql, bool NotNull);
}
