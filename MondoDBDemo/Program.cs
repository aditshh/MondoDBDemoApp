using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;

namespace MondoDBDemo
{
    class Program
    {
        static void Main(string[] args)
        {

            MongoCRUD db = new MongoCRUD("AddressBook"); // get "AddressBook" db if avail, if not avail create one.

            // this PersonModel is techincally being converted to JSON (BSON for Mongo backend terms) and insert it to the db.
            // Instead of JSON, it is BSON (Mongos way of optimizing it behind the scene)
            // Take below param and create a record in "Users" table/collection

            //PersonModel person = new PersonModel
            //{
            //    FirstName = "John",
            //    LastName = "Doe",
            //    PrimaryAddress = new AddressModel
            //    {
            //        StreetAddress = "123 Super Mama",
            //        City = "TestCity",
            //        State = "MA",
            //        Zip = "54321"
            //    }
            //};

            //db.InsertRecord("Users", person); // Mongodb is a no sql database = just store the our objects

            var recs = db.LoadRecord<PersonModel>("Users");

            foreach (var rec in recs) 
            {
                Console.WriteLine($"{ rec.FirstName } {rec.LastName }");
                
                Console.WriteLine();
            }

            //foreach (var rec in recs)
            //{
            //    Console.WriteLine($"{rec.Id} : { rec.FirstName } {rec.LastName }");
            //    if (rec.PrimaryAddress != null)
            //    {
            //        Console.WriteLine($"{ rec.PrimaryAddress.City } ");
            //    }
            //    Console.WriteLine();
            //}

            var oneRec = db.LoadRecordById<PersonModel>("Users", new Guid("0ce99c1b-e4b1-46f0-b022-c851df0aba4b"));
            oneRec.PrimaryAddress.City = "MACity";
            db.UpsertRecord<PersonModel>("Users", new Guid("0ce99c1b-e4b1-46f0-b022-c851df0aba4b"), oneRec);
            //oneRec.DateOfBirth = new DateTime(1990, 01, 01, 0, 0, 0, DateTimeKind.Utc);
            
            //db.DeleteRecord<PersonModel>("Users", oneRec.Id);

            Console.WriteLine("Hello World!");
            Console.ReadLine();
        }
    }

    [BsonIgnoreExtraElements]
    public class NameModel
    {
        [BsonId] // _id (tell Mongo that this is your unique id for this record.)
        public Guid Id { get; set; } // it is null here, mongo is smart to create a new guid when a record is a new record.
        public string  FirstName { get; set; }
        public string  LastName{ get; set; }
    }

    public class PersonModel
    {
        [BsonId] // _id (telling Mongo that this is your unique id for this record.)
        public Guid Id { get; set; } // it is null here, mongo is smart to create a new guid when a record is a new record.
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public AddressModel PrimaryAddress { get; set; }
        [BsonElement("dob")]
        public DateTime DateOfBirth { get; set; }
    }

    public class AddressModel
    {
        public string StreetAddress { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public string Zip { get; set; }
    }

    public class MongoCRUD
    {
        private IMongoDatabase db;

        public MongoCRUD(string database)
        {
            var client = new MongoClient(); // connectionstring
            // keep my option open, wether close the connection, or use the same client thru out
            db = client.GetDatabase(database); // open the connection

        }

        /// <summary>
        /// Used type "T" to be reused for different Type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="record"></param>
        public void InsertRecord<T>(string table, T record)
        {
            var collection = db.GetCollection<T>(table); // get the collection from the table.

            collection.InsertOne(record); // insert 1 record.
        }

        /// <summary>
        /// Read data (similar to Select * sql)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <returns></returns>
        public List<T> LoadRecord<T>(string table)
        {
            var collection = db.GetCollection<T>(table);

            return collection.Find(new BsonDocument()).ToList();
        }

        public T LoadRecordById<T>(string table, Guid id)
        {
            var collection = db.GetCollection<T>(table);

            var filter = Builders<T>.Filter.Eq("Id", id);
            return collection.Find(filter).First();
        }


        /// <summary>
        /// Upsert = Insert or an update, dependng on what is required. (Sql = merge statement)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="id"></param>
        /// <param name="record"></param>
        public void UpsertRecord<T>(string table, Guid id, T record)
        {
            var collection = db.GetCollection<T>(table);

            var result = collection.ReplaceOne(
                new BsonDocument("_id", id),
                record,
                new UpdateOptions { IsUpsert = true }); // IsUpsert - if no match found, insert the record. 
        }

        public void DeleteRecord<T>(string table, Guid id)
        {
            var collection = db.GetCollection<T>(table);
            var filter = Builders<T>.Filter.Eq("Id", id);
            collection.DeleteOne(filter);
        }
    }
}
