using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;

namespace MongoInvalidUTF8Bug
{
    public class Thing
    {
        [BsonId, BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }
        public string ThingData { get; set; }
        [BsonRepresentation(BsonType.ObjectId)]
        public string ThingCategoryId { get; set; }
    }

    class Program
    {
        static void OnCommandStarted(CommandStartedEvent evt)
        {
            Console.WriteLine(evt.Command.ToString());
        }

        static void OnCommandSucceeded(CommandSucceededEvent evt)
        {
            Console.WriteLine(evt.Reply.ToString());
        }

        static void OnCommandFailed(CommandFailedEvent evt)
        {
            Console.WriteLine(evt.CommandName.ToString());
        }

        static async Task TryMain()
        {
            var mongoClientSettings = MongoClientSettings.FromUrl(new MongoUrl("mongodb://localhost:27017?connect=replicaSet"));
            //mongoClientSettings.ReadEncoding = new System.Text.UTF8Encoding();
            mongoClientSettings.ClusterConfigurator = clusterConfigurator =>
            {
                clusterConfigurator.Subscribe<CommandStartedEvent>(OnCommandStarted);
                clusterConfigurator.Subscribe<CommandSucceededEvent>(OnCommandSucceeded);
                clusterConfigurator.Subscribe<CommandFailedEvent>(OnCommandFailed);
            };
            var client = new MongoClient(mongoClientSettings);
            await client.DropDatabaseAsync("testdb");

            var db = client.GetDatabase("testdb");

            var kb = Builders<Thing>.IndexKeys;
            var fb = Builders<Thing>.Filter;

            var collation = new Collation("en", strength: CollationStrength.Primary);

            var collection = db.GetCollection<Thing>("Things");
            await collection.Indexes.CreateOneAsync(
                new CreateIndexModel<Thing>(
                    kb.Combine(
                        kb.Ascending(x => x.ThingCategoryId),
                        kb.Ascending(x => x.ThingData)
                    ),
                    new CreateIndexOptions<Thing>()
                    {
                        Unique = true,

                        // ******************************************
                        // Comment out collation to 'fix' the problem
                        // ******************************************
                        Collation = collation
                    }
                )
            );

            var item = new Thing
            {
                ThingCategoryId = "61bcaba2dd1005f8f29b8a4a",
                ThingData = "/"
            };
            await collection.InsertOneAsync(item);
            item.Id = null;
            await collection.InsertOneAsync(item);
        }

        static async Task<int> Main()
        {
            try
            {
                await TryMain();
                return 0;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"Caught error {e}");
                return 1;
            }
        }
    }
}
