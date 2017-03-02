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
            string DOCDB_URL = "https://iiis-graphview-test2.documents.azure.com:443/";
            string DOCDB_AUTHKEY = "Rzxzs7fklFYQApb0VWIx2fP3AakbCBDxfuzoQrFg5Ysuh6zlKkOTzOf091fYieteKQ72qtwsdggyAq6tMN6J6w==";
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
        }
    }
}
