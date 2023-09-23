package org.jafar;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.*;
import java.net.Socket;

public class Driver {
    private Socket clientSocket;
    private OutputStream out;
    private InputStream in;
    private ObjectMapper objectMapper;

    public Driver(String host, int port) throws IOException {
        this.clientSocket = new Socket(host, port);
        this.out = clientSocket.getOutputStream();
        this.in = clientSocket.getInputStream();
        this.objectMapper = new ObjectMapper();
    }

    public String createDatabase(String databaseName) throws IOException {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("name", databaseName);
        objectNode = sendToDb(objectNode, WireProtocol.OperationType.CREATE_DB);
        return objectNode.get("response").asText();
    }

    public String createCollection(String collectionName, ObjectNode schema) throws IOException {
        schema.put("collectionName", collectionName);
        ObjectNode objectNode = sendToDb(schema, WireProtocol.OperationType.CREATE_COLLECTION);
        return objectNode.get("response").asText();
    }

    public String deleteCollection(String collectionName) throws IOException {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("name", collectionName);
        objectNode = sendToDb(objectNode, WireProtocol.OperationType.DELETE_COLLECTION);
        return objectNode.get("response").asText();
    }

    public String deleteDatabase(String databaseName) throws IOException {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("name", databaseName);
        objectNode = sendToDb(objectNode, WireProtocol.OperationType.DELETE_DATABASE);
        return objectNode.get("response").asText();
    }

    public String getAllDatabases() throws IOException {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode = sendToDb(objectNode, WireProtocol.OperationType.SHOW_DATABASES);
        return objectNode.get("response").toString();
    }

    public String getAllCollections() throws IOException {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode = sendToDb(objectNode, WireProtocol.OperationType.SHOW_COLLECTIONS);
        return objectNode.get("response").toString();
    }

    public String insert(String collectionName, ObjectNode obj) throws IOException {
        obj.put("collectionName", collectionName);
        ObjectNode objectNode = sendToDb(obj, WireProtocol.OperationType.INSERT);
        return objectNode.get("response").asText();
    }

    public ObjectNode update(ObjectNode objectNode) throws IOException {
        return sendToDb(objectNode, WireProtocol.OperationType.UPDATE);
    }

    public ObjectNode delete(ObjectNode objectNode) throws IOException {
        return sendToDb(objectNode, WireProtocol.OperationType.DELETE);
    }

    public ObjectNode get() throws IOException {
        return sendToDb(null, WireProtocol.OperationType.QUERY);
    }

    public JsonNode getAll(String collectionName) throws IOException {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("name", collectionName);
        objectNode = sendToDb(objectNode, WireProtocol.OperationType.QUERY);
        return objectNode.get("response");
    }

    private ObjectNode sendToDb(ObjectNode obj, WireProtocol.OperationType operationType) throws IOException {
        byte[] jsonData = obj != null ? objectMapper.writeValueAsBytes(obj): new byte[0];
        WireProtocol.Message message = WireProtocol.createMessage(operationType, jsonData);

        out.write(message.serialize());
        message = WireProtocol.createMessage(in);
        return (ObjectNode) objectMapper.readTree(message.getPayload());
    }

    public void close() throws IOException {
        this.clientSocket.close();
    }

    public static class Response {
        private WireProtocol.Header header;
        private byte[] data;

        public Response(WireProtocol.Header header, byte[] data) {
            this.header = header;
            this.data = data;
        }

        public WireProtocol.Header getHeader() {
            return header;
        }

        public byte[] getData() {
            return data;
        }

        public boolean isSuccess() {
            // You might want to define a field in the header or a structure in the payload to indicate success or error
            return true;  // Placeholder
        }
    }
}