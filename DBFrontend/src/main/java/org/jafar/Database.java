package org.jafar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Database {
    private final DB db;
    private final ObjectMapper objectMapper;


    public Database(String path) throws IOException, Constants.NotJafarDBFile {
        db = DB.open("data/" + path + ".db", new Options());
        objectMapper = new ObjectMapper();
    }

    public static List<String> getAllDatabases() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get("data"), "*.db")) {
            return StreamSupport.stream(stream.spliterator(), false)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .map(filename -> filename.replace(".db", ""))
                    .collect(Collectors.toList());
        }
    }

    public void createCollection(ObjectNode schema) throws Constants.WriteInsideReadTransactionException, IOException {
        String collectionName = schema.remove("collectionName").asText();

        Transaction transaction = db.writeTransaction();
        Collection collection = transaction.createCollection(collectionName.getBytes());

        List<String> fieldNames = new ArrayList<>();
        schema.fieldNames().forEachRemaining(fieldNames::add);
        collection.put("schema".getBytes(), new ObjectMapper().writeValueAsBytes(fieldNames));

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

    public boolean deleteDatabase(String name) throws IOException {
        Path filePath = Paths.get("data", name);
        return Files.deleteIfExists(filePath);
    }

    public boolean deleteCollection(String collectionName) throws Constants.WriteInsideReadTransactionException, IOException {
        Transaction transaction = db.writeTransaction();
        boolean result = transaction.deleteCollection(collectionName.getBytes());
        transaction.commit();
        return result;
    }

    public boolean insertDocument(ObjectNode document) throws IOException, Constants.WriteInsideReadTransactionException {
        String collectionName = document.remove("collectionName").asText();

        Transaction transaction = db.writeTransaction();
        Optional<Collection> collection = transaction.getCollection(collectionName.getBytes());
        if (collection.isEmpty()) {
            throw new IOException();
        }

        // schema must exist!
        byte[] value = collection.get().find("schema".getBytes()).get().value();
        List<String> fieldNames = new ObjectMapper().readValue(value, new TypeReference<List<String>>() {});
        if (fieldNames.size() != document.size()) return false;

        for (String field: fieldNames) {
            if (!document.has(field)) return false;
        }

        String key = UUID.randomUUID().toString();
        document.put("id", key);
        collection.get().put(key.getBytes(), document.toString().getBytes());
        transaction.commit();
        return true;
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
