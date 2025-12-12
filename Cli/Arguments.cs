using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SenteApp.Cli
{
    public static class Arguments
    {
        public static string GetRequired(string[] args, string name)
        {
            var idx = Array.IndexOf(args, name);
            if (idx == -1 || idx + 1 >= args.Length)
                throw new ArgumentException($"Brak wymaganego parametru {name}");

            return args[idx + 1];
        }
    }
}
