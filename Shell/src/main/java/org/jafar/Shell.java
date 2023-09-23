package org.jafar;
import java.io.IOException;
import java.util.Scanner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Shell {
    private String currentDatabase = null;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static Driver driver;

    public Shell(String host, int port) {
        try {
            driver = new Driver(host, port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String useDatabase(String dbName) {
        try {
            return driver.createDatabase(dbName);
        } catch (IOException e) {
            return "Driver failed to send message";
        }
    }

    private String createCollection(String collectionName, ObjectNode jsonNode) {
        try {
            return driver.createCollection(collectionName, jsonNode);
        } catch (IOException e) {
            return "Driver failed to send message";
        }
    }

    private String deleteDatabase(String databaseName) {
        try {
            return driver.deleteDatabase(databaseName);
        } catch (IOException e) {
            return "Driver failed to send message";
        }
    }

    private String deleteCollection(String collectionName) {
        try {
            return driver.deleteCollection(collectionName);
        } catch (IOException e) {
            return "Driver failed to send message";
        }
    }

    private String showDatabases() {
        try {
            return driver.getAllDatabases();
        } catch (IOException e) {
            return "Driver failed to send message";
        }
    }

    private String showCollections() {
        try {
            return driver.getAllCollections();
        } catch (IOException e) {
            return "Driver failed to send message";
        }
    }

    private String insertIntoCollection(String collectionName, ObjectNode json) {
        try{
            return driver.insert(collectionName, json);
        } catch (IOException e) {
            return "Driver failed to send message";
        }
    }

    public String getAllFromCollection(String collectionName) {
        try {
            return driver.getAll(collectionName).toString();
        } catch (IOException e) {
            return "Driver failed to send message";
        }
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("SimpleDB Shell v1.0");
        System.out.println("Type 'exit' to exit.");

        while (true) {
            System.out.print("> ");
            String command = scanner.nextLine().trim();

            if (command.equalsIgnoreCase("exit")) {
                System.out.println("Exiting SimpleDB Shell...");
                break;
            } else if (command.startsWith("show databases")) {
                System.out.println(showDatabases());
            } else if (command.startsWith("use database ")) {
                System.out.println(useDatabase(command.split(" ")[2]));
            } else if (command.startsWith("delete database ")) {
                System.out.println(deleteDatabase(command.split(" ")[2]));
            } else if (command.startsWith("show collections")) {
                System.out.println(showCollections());
            } else if (command.startsWith("create collection ")) {
                String collectionName = command.split(" ")[2];
                ObjectNode jsonNode = parseJson(scanner);
                if (jsonNode == null) continue;
                System.out.println(createCollection(collectionName, jsonNode));
            } else if (command.startsWith("delete collection ")) {
                System.out.println(deleteCollection(command.split(" ")[2]));
            } else if (command.startsWith("insert into ")) {
                String collectionName = command.split(" ")[2];
                ObjectNode jsonNode = parseJson(scanner);
                if (jsonNode == null) continue;
                System.out.println(insertIntoCollection(collectionName, jsonNode));
            } else if (command.startsWith("get all from ")) {
                String collectionName = command.split(" ")[2];
                System.out.println(getAllFromCollection(collectionName));
            } else {
                System.out.println("Unknown command: " + command);
            }
        }

        scanner.close();
    }

    public static ObjectNode parseJson(Scanner scanner) {
        System.out.println("Please enter a valid JSON string:");

        String jsonString = scanner.nextLine();

        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(jsonString);
            if (!jsonNode.isObject()) {
                throw new IOException();
            }
        } catch (IOException e) {
            System.out.println("Invalid JSON Object");
            return null;
        }

        return (ObjectNode) jsonNode;
    }
}