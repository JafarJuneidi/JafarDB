package org.jafar;
import java.io.IOException;
import java.util.Scanner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Shell {

    private String currentDatabase = null;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static Driver driver;

    private void useDatabase(String dbName) {
        String response;
        try {
            response = driver.createDatabase(dbName);
        } catch (IOException e) {
            response = "Driver failed to send message";
        }
        System.out.println(response);
    }

    private void createCollection(String collectionName) {
        String response;
        try {
            response = driver.createCollection(collectionName);
        } catch (IOException e) {
            response = "Driver failed to send message";
        }
        System.out.println(response);
    }

    private void deleteCollection(String collectionName) {
        String response;
        try {
            response = driver.deleteCollection(collectionName);
        } catch (IOException e) {
            response = "Driver failed to send message";
        }
        System.out.println(response);
    }

    private void showCollections() {
        String response;
        try {
            response = driver.getAllCollections();
        } catch (IOException e) {
            response = "Driver failed to send message";
        }
        System.out.println(response);
    }

    private void insertIntoCollection(String collectionName, ObjectNode json) {
        String response;
        try {
            response = driver.insert(collectionName, json);
        } catch (IOException e) {
            response = "Driver failed to send message";
        }
        System.out.println(response);
    }

    public void getAllFromCollection(String collectionName) {
        try {
            JsonNode response = driver.getAll(collectionName);
            System.out.println(response);
        } catch (IOException e) {
            System.out.println("Driver failed to send message");
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
            } else if (command.startsWith("use database ")) {
                useDatabase(command.split(" ")[2]);
            } else if (command.startsWith("create collection ")) {
                createCollection(command.split(" ")[2]);
            } else if (command.startsWith("delete collection ")) {
                deleteCollection(command.split(" ")[2]);
            } else if (command.startsWith("insert into ")) {
                String collectionName = command.split(" ")[2];
                ObjectNode jsonNode = parseJson(scanner);
                if (jsonNode == null) continue;
                insertIntoCollection(collectionName, jsonNode);
            } else if (command.startsWith("get all from ")) {
                String collectionName = command.split(" ")[2];
                getAllFromCollection(collectionName);
            } else if (command.startsWith("show collections")) {
                showCollections();
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

        System.out.println("Parsed JSON: " + jsonNode.toString());

        return (ObjectNode) jsonNode;
    }

    public static void main(String[] args) {
        try {
            driver = new Driver("localhost", 12345);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Shell shell = new Shell();
        shell.start();
    }
}
