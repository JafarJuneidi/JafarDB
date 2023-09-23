package org.jafar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Server {
    private static final int PORT = 9000;
    private static final String myHost = System.getenv("SERVICE_NAME");
    private final String[] hosts = {"jafardb-node1-1", "jafardb-node2-1"};
    private final Map<String, Socket> sockets = new HashMap<>();

    public void connectToHosts() throws IOException {
        for (String host: hosts) {
            if (Objects.equals(host, myHost)) continue;
            sockets.put(host, new Socket(host, PORT));
        }
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Node started on: " + myHost + ":" + serverSocket.getLocalPort());
//            connectToHosts();

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from: " + clientSocket.getInetAddress());

                // Spawn a new thread to handle this client
                ClientHandler clientHandler = new ClientHandler(clientSocket, myHost, sockets);
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
    private final String myHost;
    private final Map<String, Socket> sockets;

    public ClientHandler(Socket clientSocket, String myHost, Map<String, Socket> sockets) {
        this.clientSocket = clientSocket;
        this.objectMapper = new ObjectMapper();
        this.myHost = myHost;
        this.sockets = sockets;
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

                if (message.getOperationType() != WireProtocol.OperationType.CREATE_DB &&
                        message.getOperationType() != WireProtocol.OperationType.SHOW_DATABASES &&
                        database == null) {
                    objectNode = objectMapper.createObjectNode();
                    objectNode.put("response", "Database was not selected!");
                    message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                    out.write(message.serialize());
                }

                switch (message.getOperationType()) {
                    case CREATE_DB -> {
                        try {
                            database = new Database(objectNode.get("name").asText());
                            broadcast(message);
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", "Database Added successfully");
                        } catch (IOException | Constants.NotJafarDBFile e) {
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", e.getMessage());
                        }
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                    case SHOW_DATABASES -> {
                        List<String> databases = Database.getAllDatabases();
                        objectNode = objectMapper.createObjectNode();
                        ArrayNode arrayNode = objectMapper.valueToTree(databases);
                        objectNode.set("response", arrayNode);
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                    case DELETE_DATABASE -> {
                        boolean result = database.deleteDatabase(objectNode.get("name").asText());
                        objectNode = objectMapper.createObjectNode();
                        if (result) {
                            objectNode.put("response", "Database deleted successfully");
                            broadcast(message);
                        } else {
                            objectNode.put("response", "Failed to delete database");
                        }
                        message = WireProtocol.createMessage(WireProtocol.OperationType.RESPONSE, objectMapper.writeValueAsBytes(objectNode));
                        out.write(message.serialize());
                    }
                    case CREATE_COLLECTION -> {
                        try {
                            database.createCollection(objectNode.deepCopy());
                            broadcast(message);
                            objectNode = objectMapper.createObjectNode();
                            objectNode.put("response", "Collection Added successfully");
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
                    case DELETE_COLLECTION -> {
                        try {
                            boolean result = database.deleteCollection(objectNode.get("name").asText());
                            objectNode = objectMapper.createObjectNode();
                            if (result) {
                                objectNode.put("response", "Collection deleted successfully");
                                broadcast(message);
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
                    case INSERT -> {
                        try {
                            boolean result = database.insertDocument(objectNode.deepCopy());
                            objectNode = objectMapper.createObjectNode();
                            if (result) {
                                objectNode.put("response", "Document Added successfully");
                                broadcast(message);
                            } else {
                                objectNode.put("response", "Schema doesn't match!");
                            }
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
                            broadcast(message);
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
                if (database != null) database.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void broadcast(WireProtocol.Message message) throws IOException {
        if (message.getIsBroadcast()) return;
        message.setBroadcast(true);

        for (Map.Entry<String, Socket> entry: sockets.entrySet()) {
            if (Objects.equals(entry.getKey(), myHost)) continue;
            System.out.println("Broadcasting to " + entry.getKey());

            OutputStream forwardOut = entry.getValue().getOutputStream();
            forwardOut.write(message.serialize());
        }
    }
}
