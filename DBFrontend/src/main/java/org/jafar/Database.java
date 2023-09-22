package org.jafar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class Database {
    private final DB db;
    private final ObjectMapper objectMapper;


    public Database(String path) throws IOException, Constants.NotJafarDBFile {
        db = DB.open("data/" + path + ".db", new Options());
        objectMapper = new ObjectMapper();
    }

    public void createCollection(String collectionName) throws Constants.WriteInsideReadTransactionException, IOException {
        Transaction transaction = db.writeTransaction();
        transaction.createCollection(collectionName.getBytes());
        transaction.commit();
    }

    public List<String> getAllCollections() throws IOException {
        Transaction transaction = db.readTransaction();
        Collection rootCollection = transaction.getRootCollection();
        transaction.commit();
        List<Item> items = rootCollection.findAll();
        return items.stream().map(item -> new String(item.key())).collect(Collectors.toList());
    }

    public boolean doesCollectionExist(String collectionName) {
        Transaction transaction = db.readTransaction();
        try {
            Optional<Collection> collection = transaction.getCollection(collectionName.getBytes());
            transaction.commit();
            return collection.isPresent();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean deleteCollection(String collectionName) throws Constants.WriteInsideReadTransactionException, IOException {
        Transaction transaction = db.writeTransaction();
        boolean result = transaction.deleteCollection(collectionName.getBytes());
        transaction.commit();
        return result;
    }

    public void insertDocument(ObjectNode document, String collectionName) throws IOException, Constants.WriteInsideReadTransactionException {
        Transaction transaction = db.writeTransaction();
        Optional<Collection> collection = transaction.getCollection(collectionName.getBytes());
        if (collection.isEmpty()) {
            throw new IOException();
        }

        String key = UUID.randomUUID().toString();
        document.put("id", key);
        collection.get().put(key.getBytes(), document.toString().getBytes());
        transaction.commit();
    }

    public void updateDocument(ObjectNode document, String collectionName) throws IOException, Constants.WriteInsideReadTransactionException {
        Transaction transaction = db.writeTransaction();
        Optional<Collection> collection = transaction.getCollection(collectionName.getBytes());
        if (collection.isEmpty()) {
            throw new IOException();
        }

        collection.get().put(UUID.randomUUID().toString().getBytes(), document.toString().getBytes());
        transaction.commit();
    }

    public Optional<Item> getDocumentById(byte[] key, String collectionName) throws IOException {
        Transaction transaction = db.readTransaction();
        Optional<Collection> collection = transaction.getCollection(collectionName.getBytes());
        if (collection.isEmpty()) {
            throw new IOException();
        }

        return collection.get().find(key);
    }

    public List<JsonNode> getAllDocuments(String collectionName) throws IOException {
        Transaction transaction = db.readTransaction();
        Optional<Collection> collection = transaction.getCollection(collectionName.getBytes());
        if (collection.isEmpty()) {
            throw new IOException();
        }

        List<Item> items = collection.get().findAll();
        return items.stream().map(item -> {
            try {
                return objectMapper.readTree(item.value());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).toList();
    }

    public void close() throws IOException {
        db.close();
    }
}
