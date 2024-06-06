/**
 * Oleksandr Kononenko
 * <p>
 * Copyright (c) 1993-1996 Sun Microsystems, Inc. All Rights Reserved.
 * <p>
 * This software is the confidential and proprietary information of Sun
 * Microsystems, Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Sun.
 * <p>
 * SUN MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF
 * THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. SUN SHALL NOT BE LIABLE FOR
 * ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */
package kononenko.nosqlproject;

import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;


public class MongoDBApp {

    private static MongoClient client;

    /**
     * The {@code Main} serves to run the application and check work function.
     *
     * @author Oleksandr Kononenko
     * @version 1.10 06 Jul 2024
     */
    public static void main(String[] args) {
        client = MongoClients.create("mongodb://localhost:27017");  // або URI для хмарного сервера

        client.listDatabases();
        listCollections("test_db");
        listDocuments("test_db", "test_collection");
        listDocumentsWithFilter("test_db", "test_collection", eq("name", "John"));

        insertOneDocument("test_db", "test_collection", new Document("name", "Alice").append("age", 30));
        insertManyDocuments("test_db", "test_collection", List.of(
                new Document("name", "Bob").append("age", 25),
                new Document("name", "Charlie").append("age", 35)
        ));

        updateOneDocument("test_db", "test_collection", eq("name", "Alice"), set("age", 31));
        updateManyDocuments("test_db", "test_collection", eq("age", 25), set("status", "active"));

        deleteOneDocument("test_db", "test_collection", eq("name", "Charlie"));
        deleteManyDocuments("test_db", "test_collection", eq("status", "active"));
    }

    /**
     * Method for displaying all collections in a given database.
     *
     * @param dbName name DB
     * @return string with dbName & collectionName
     */
    public static void listCollections(String dbName) {
        MongoDatabase database = client.getDatabase(dbName);
        MongoIterable<String> collections = database.listCollectionNames();
        for (String collectionName : collections) {
            System.out.println("Collection in " + dbName + ": " + collectionName);
        }
    }


    /**
     * A method for displaying all documents from a given collection.
     *
     * @param dbName         name DB
     * @param collectionName name Collection
     * @return string with all documents from db
     */
    public static void listDocuments(String dbName, String collectionName) {
        MongoDatabase database = client.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        try (MongoCursor<Document> cursor = collection.find().iterator()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }
    }

    /**
     * A method for outputting documents using filters.
     *
     * @param dbName         name DB
     * @param collectionName name Collection
     * @param filter         filter for documents
     * @return Returns string filtered documents
     */
    public static void listDocumentsWithFilter(String dbName, String collectionName, Bson filter) {
        MongoDatabase database = client.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        try (MongoCursor<Document> cursor = collection.find(filter).iterator()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }
    }

    /**
     * Method for adding a single document.
     *
     * @param dbName         name DB
     * @param collectionName name Collection
     * @param document       name document
     * @return string name document inserted
     */
    public static void insertOneDocument(String dbName, String collectionName, Document document) {
        MongoDatabase database = client.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertOne(document);
        System.out.println("Inserted document: " + document.toJson());
    }

    /**
     * Method for adding a group of documents.
     *
     * @param dbName         name DB
     * @param collectionName name Collection
     * @param documents      list names document
     * @return string name group of documents inserted
     */
    public static void insertManyDocuments(String dbName, String collectionName, List<Document> documents) {
        MongoDatabase database = client.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.insertMany(documents);
        System.out.println("Inserted documents: " + documents);
    }

    /**
     * Method for editing a single document.
     *
     * @param dbName         name DB
     * @param collectionName name Collection
     * @param filter         filter for documents
     * @param update         update information
     * @return string name of document updated
     */
    public static void updateOneDocument(String dbName, String collectionName, Bson filter, Bson update) {
        MongoDatabase database = client.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.updateOne(filter, update);
        System.out.println("Updated one document with filter: " + filter.toBsonDocument().toJson());
    }

    /**
     * Method for editing and a group of documents.
     *
     * @param dbName         name DB
     * @param collectionName name Collection
     * @param filter         filter for documents
     * @param update         update information
     * @return string names of documents updated
     */
    public static void updateManyDocuments(String dbName, String collectionName, Bson filter, Bson update) {
        MongoDatabase database = client.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.updateMany(filter, update);
        System.out.println("Updated many documents with filter: " + filter.toBsonDocument().toJson());
    }

    /**
     * Method for deleting a single document.
     *
     * @param dbName         name DB
     * @param collectionName name Collection
     * @param filter         filter for documents
     * @return string name document then be deleted
     */

    public static void deleteOneDocument(String dbName, String collectionName, Bson filter) {
        MongoDatabase database = client.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteOne(filter);
        System.out.println("Deleted one document with filter: " + filter.toBsonDocument().toJson());
    }

    /**
     * Method for deleting a group of documents.
     *
     * @param dbName         name DB
     * @param collectionName name Collection
     * @param filter         filter for documents
     * @return string names documents then be deleted
     */
    public static void deleteManyDocuments(String dbName, String collectionName, Bson filter) {
        MongoDatabase database = client.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteMany(filter);
        System.out.println("Deleted many documents with filter: " + filter.toBsonDocument().toJson());
    }
}