using GraphView;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace QuickStart
{
    class Program
    {
        static void Main(string[] args)
        {
            // Azure DocumentDB configuration
            string DOCDB_URL = "https://localhost:8081/";
            string DOCDB_AUTHKEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            string DOCDB_DATABASE = "NetworkS";
            string DOCDB_COLLECTION = "ontest";

            // create collection
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            connection.ResetCollection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            // add nodes and edges
            graph.g().AddV("student").Property("number", "123").Next();
            graph.g().AddV("student").Property("number", "456").Next();
            graph.g().AddV("class").Property("name", "network").Next();

            graph.g().V().Has("number", "123").
                             AddE("takes").Property("credit", 3).
                             To(graph.g().V().Has("name", "network")).Next();

            // query
            var res = graph.g().V().Has("number", "123").Out().Values("name").Next();
            foreach (var x in res)
            {
                System.Console.WriteLine(x);
            }
            System.Console.ReadKey();
        }
    }
}
