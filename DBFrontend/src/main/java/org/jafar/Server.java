package org.jafar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jafar.WireProtocol.*;
import org.jafardb.Constants;

import java.io.*;
import java.net.*;
import java.util.List;

public class Server {
    private static final int PORT = 12345;

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server started on port: " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from: " + clientSocket.getInetAddress());

                // Spawn a new thread to handle this client
                ClientHandler clientHandler = new ClientHandler(clientSocket);
                new Thread(clientHandler).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final ObjectMapper objectMapper;
    private Database database;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void run() {
        try (
                InputStream in = clientSocket.getInputStream();
                OutputStream out = clientSocket.getOutputStream()
        ) {
            while (true) {
                WireProtocol.Message message = WireProtocol.createMessage(in);
                ObjectNode objectNode = (ObjectNode) objectMapper.readTree(message.getPayload());

                if (message.getOperationType() != WireProtocol.OperationType.CREATE_DB && database == null) {
                    objectNode = objectMapper.createObjectNode();
                    objectNode.put("response", "Database was not selected!");
                    message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                    out.write(message.serialize());
                }

                switch (message.getOperationType()) {
                    case CREATE_DB -> {
                        try {
                            database = new Database(objectNode.get("name").asText());
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", "Database Added successfully");
                        } catch (IOException | Constants.NotJafarDBFile e) {
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", e.getMessage());
                        }
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                    case CREATE_COLLECTION -> {
                        try {
                            database.createCollection(objectNode.get("name").asText());
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", "Collection Added successfully");
                        } catch (Constants.WriteInsideReadTransactionException e) {
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", e.getMessage());
                        }
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                    case DELETE_COLLECTION -> {
                        try {
                            boolean result = database.deleteCollection(objectNode.get("name").asText());
                            objectNode = objectMapper.createObjectNode();
                            if (result) {
                                objectNode.put("response", "Collection deleted successfully");
                            } else {
                                objectNode.put("response", "Failed to delete collection");
                            }
                        } catch (Constants.WriteInsideReadTransactionException e) {
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", e.getMessage());
                        }
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                    case SHOW_COLLECTIONS -> {
                        List<String> collections = database.getAllCollections();
                        objectNode = objectMapper.createObjectNode();
                        ArrayNode arrayNode = objectMapper.valueToTree(collections);
                        objectNode.set("response", arrayNode);
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                    case INSERT -> {
                        try {
                            database.insertDocument(objectNode, "users");
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", "Document Added successfully");
                        } catch (Constants.WriteInsideReadTransactionException e) {
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", e.getMessage());
                        }
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                    case UPDATE -> {
                        try {
                            database.updateDocument(objectNode, "users");
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", "Document Added successfully");
                        } catch (Constants.WriteInsideReadTransactionException e) {
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", e.getMessage());
                        }
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                    case QUERY -> {
                        List<JsonNode> items = database.getAllDocuments("users");
                        objectNode = objectMapper.createObjectNode();
                        ArrayNode arrayNode = objectMapper.valueToTree(items);
                        objectNode.set("response", arrayNode);
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                database.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
