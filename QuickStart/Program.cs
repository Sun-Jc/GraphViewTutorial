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
        static void print(string label, List<string> res)
        {
            System.Console.WriteLine(label);
            foreach (var x in res)
            {
                System.Console.WriteLine(x);
            }
            System.Console.WriteLine();
        }

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
            graph.g().AddV("person").Property("age", "27").Property("name", "vadas").Next();
            graph.g().AddV("person").Property("age", "29").Property("name", "marko").Next();
            graph.g().AddV("person").Property("age", "35").Property("name", "peter").Next();
            graph.g().AddV("person").Property("age", "32").Property("name", "josh").Next();
            graph.g().AddV("software").Property("lang", "java").Property("name", "lop").Next();
            graph.g().AddV("software").Property("lang", "java").Property("name", "ripple").Next();

            graph.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5).To(graph.g().V().Has("name", "vadas")).Next();
            graph.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0).To(graph.g().V().Has("name", "josh")).Next();
            graph.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4).To(graph.g().V().Has("name", "lop")).Next();
            graph.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2).To(graph.g().V().Has("name", "lop")).Next();
            graph.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4).To(graph.g().V().Has("name", "lop")).Next();
            graph.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0).To(graph.g().V().Has("name", "ripple")).Next();

            // query

            // 1) ask GraphView what does "marko" created
            var res = graph.g().V().Has("name", "marko").Out("created").Values("name").Next();
            print("1)", res);

            // 1*) ask GraphView how many persons have ever created any software?
            res = graph.g().V().HasLabel("software").In("created").Values("name").Dedup().Count().Next();
            print("1*)", res);


            // 2) ask GraphView Who are the people that marko develops software with? (excluding marko)
            res = graph.g().V().Has("name", "marko").As("exclude").Out("created").In("created").Where(Predicate.neq("exclude")).Values("name").Next();
            print("2)", res);

            // 2*) ask GraphView the name of nodes that have an edges pointing to "lop" with weight larger then 0.3 
            res = graph.g().V().HasLabel("software").Has("name", "lop").InE().Where(GraphTraversal2.__().Values("weight").Is(Predicate.gt(0.3))).OutV().Values("name").Next();
            print("2*)", res);

            // 3) ask GraphView the average age of "vadas" and "marko"
              print("3)", res);

            // 4) ask GraphView what common software(out neighbor) do "josh" and "peter" share?
            res = graph.g().V().Has("name", "josh").Out("created").As("common").In("created").Has("name", "peter").Select("common").Values("name").Next();
            print("4)", res);

            // 4+) same as 4)
            res = graph.g().V().Has("name", "josh").Out("created").Where(GraphTraversal2.__().In("created").Has("name", "peter")).Values("name").Next();
            print("4+)", res);
            
            // 5) ask GraphView all the 3-hop paths starting from "marko"
            res = graph.g().V().Has("name", "marko").As("a").Both().As("b").Both().
                Where(Predicate.neq("a")).Both().Where(Predicate.neq("b")).Path().By("name").Next();
            print("5)", res);

            // 6) change the age of "marko" into 25
            graph.g().V().Has("name", "marko").Property("age", "25").Next();
            res = graph.g().V().Has("name", "marko").Values("age").Next();
            print("6)", res);

            // 7) delete node "vadas" and all of its adjancent edges
            graph.g().V().Has("name", "vadas").Drop().Next();            
            res = graph.g().V().Has("name", "vadas").Next();
            print("7)", res);
            graph.g().AddV("person").Property("age", "27").Property("name", "vadas").Next();
            graph.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5).To(graph.g().V().Has("name", "vadas")).Next();

            // 8*) ask GraphView "marko"'s 2-hop destinations
            res = graph.g().V().Has("name", "marko").
                Repeat(GraphTraversal2.__().Out()).Times(2).
                Values("name").Next();
            print("8)", res);

            // 9*) for every person, if possible, find out what is created by who this person knows; output the type and name of every result
            res = graph.g().V().HasLabel("person").
                Optional(GraphTraversal2.__().Out("knows")).
                Optional(GraphTraversal2.__().Out("created")).
                Dedup().
                Project("type","name").
                By(GraphTraversal2.__().Label()).
                By(GraphTraversal2.__().Values("name")).Next();
            print("9)", res);


            // 10*) give out all the paths while traveling from "marko" within 2 hops
            res = graph.g().V().Has("name", "marko").Emit().
                Repeat(GraphTraversal2.__().Out().As("X")).Times(2).Path().By("name").Next();
            print("10*)", res);

            // 11*) Lowest Common Ancestor of A & D
            graph.g().AddV("n").Property("name", "A").Next();
            graph.g().AddV("n").Property("name", "B").Next();
            graph.g().AddV("n").Property("name", "C").Next();
            graph.g().AddV("n").Property("name", "D").Next();
            graph.g().AddV("n").Property("name", "E").Next();
            graph.g().AddV("n").Property("name", "F").Next();
            graph.g().AddV("n").Property("name", "G").Next();

            graph.g().V().Has("name", "A").AddE("knows").To(graph.g().V().Has("name", "B")).Next();
            graph.g().V().Has("name", "B").AddE("knows").To(graph.g().V().Has("name", "C")).Next();
            graph.g().V().Has("name", "D").AddE("knows").To(graph.g().V().Has("name", "C")).Next();
            graph.g().V().Has("name", "C").AddE("knows").To(graph.g().V().Has("name", "E")).Next();
            graph.g().V().Has("name", "E").AddE("knows").To(graph.g().V().Has("name", "F")).Next();
            graph.g().V().Has("name", "G").AddE("knows").To(graph.g().V().Has("name", "F")).Next();

            res = graph.g().V().Has("name", "A").
               Repeat(GraphTraversal2.__().Out()).
               Emit().As("x").Repeat(GraphTraversal2.__().In()).
               Emit(GraphTraversal2.__().Has("name", "D")).
               Select("x").Limit(1).Values("name").Next();
            print("11*)", res);

            // 12) Order "created" edges by their weights
            res = graph.g().E().HasLabel("created").Order().By("weight").
                Project("from","to","weight").By(GraphTraversal2.__().OutV().Values("name")).
                By(GraphTraversal2.__().InV().Values("name")).
                By(GraphTraversal2.__().Values("weight")).Next();       
            print("12)", res);

            // 13) Simple path
            res = graph.g().V().HasLabel("person").Both().Both().SimplePath().Path().By("name").Next();
            print("x", res);

            System.Console.WriteLine("Finished");
            System.Console.ReadKey();

            /*
            graph.g().AddV("n").Property("name", "A").Next();
            graph.g().AddV("n").Property("name", "B").Next();
            graph.g().AddV("n").Property("name", "C").Next();
            graph.g().AddV("n").Property("name", "D").Next();
            graph.g().AddV("n").Property("name", "E").Next();
            graph.g().AddV("n").Property("name", "F").Next();
            graph.g().AddV("n").Property("name", "G").Next();

            graph.g().V().Has("name", "A").AddE("knows").To(graph.g().V().Has("name", "B")).Next();
            graph.g().V().Has("name", "B").AddE("knows").To(graph.g().V().Has("name", "C")).Next();
            graph.g().V().Has("name", "D").AddE("knows").To(graph.g().V().Has("name", "C")).Next();
            graph.g().V().Has("name", "C").AddE("knows").To(graph.g().V().Has("name", "E")).Next();
            graph.g().V().Has("name", "E").AddE("knows").To(graph.g().V().Has("name", "F")).Next();
            graph.g().V().Has("name", "G").AddE("knows").To(graph.g().V().Has("name", "F")).Next();

            res = graph.g().V().Has("name","A").
                Repeat(GraphTraversal2.__().Out()).
                Emit().As("x").Repeat(GraphTraversal2.__().In()).
                Emit(GraphTraversal2.__().Has("name","D")).
                Select("x").Limit(2).Values("name").Next();
            */

            /*
             * res = graph.g().V().HasLabel("person").
                Choose(GraphTraversal2.__().Values("age").Is(Predicate.lte(30)), GraphTraversal2.__().In(), GraphTraversal2.__().Out()).Values("name");
                
             * res = graph.g().V().Match(
                GraphTraversal2.__().As("a").Out().As("b"),
                GraphTraversal2.__().As("b").Has("name", "lop"),
                GraphTraversal2.__().As("b").In("created").As("c"),
                GraphTraversal2.__().As("c").Has("age", 29))
                .Select("a").Next();
            */
        }
    }
}
