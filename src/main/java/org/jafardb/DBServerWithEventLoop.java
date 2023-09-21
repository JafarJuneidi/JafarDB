package org.jafardb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jafar.WireProtocol;

import java.io.InputStream;
import java.io.OutputStream;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class DBServerWithEventLoop {
    private ServerSocket serverSocket;
    private ExecutorService threadPool;
    private BlockingQueue<RequestTask> requestQueue;
    private BlockingQueue<ResponseTask> responseQueue;
    private ObjectMapper objectMapper;
    private DB db;

    public DBServerWithEventLoop(int port, int poolSize) throws IOException {
        serverSocket = new ServerSocket(port);
        threadPool = Executors.newFixedThreadPool(poolSize);
        requestQueue = new LinkedBlockingQueue<>();
        responseQueue = new LinkedBlockingQueue<>();
        objectMapper = new ObjectMapper();
    }

    public void start() {
        // Start the request handler
        new Thread(() -> {
            while (true) {
                try {
                    RequestTask task = requestQueue.take();
                    threadPool.submit(() -> processRequest(task));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();

        // Start the response handler
        new Thread(() -> {
            while (true) {
                try {
                    ResponseTask task = responseQueue.take();
                    sendResponse(task);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();

        // Accept incoming client connections
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                requestQueue.put(new RequestTask(clientSocket));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void processRequest(RequestTask task) {
        // Handle the request from the client (e.g., read the message, interact with the DB, etc.)
        // Once the request is processed, put the result in the response queue
//        ResponseTask responseTask = /* ... */;
//        responseQueue.put(responseTask);
    }

    private void sendResponse(ResponseTask task) {
        // Send the response back to the client
    }

    // Request and Response task classes
    private static class RequestTask {
        Socket clientSocket;

        RequestTask(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }
    }

    private static class ResponseTask {
        Socket clientSocket;
        byte[] response;

        ResponseTask(Socket clientSocket, byte[] response) {
            this.clientSocket = clientSocket;
            this.response = response;
        }
    }

    private void handleClient(Socket clientSocket) {
        try (InputStream in = clientSocket.getInputStream(); OutputStream out = clientSocket.getOutputStream()) {
            byte[] headerData = new byte[4]; // size of head
            in.read(headerData);
            WireProtocol.Header header = deserializeHeader(headerData);

            byte[] payload = new byte[header.getPayloadLength()];
            in.read(payload);

            // Process the message and get a response
            byte[] responsePayload = processMessage(header, payload);

            WireProtocol.Header responseHeader = new WireProtocol.Header(WireProtocol.OperationType.RESPONSE, responsePayload.length);
            byte[] responseMessage = combine(responseHeader, responsePayload);

            out.write(responseMessage);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] processMessage(WireProtocol.Header header, byte[] payload) {
        switch (header.getOperationType()) {
//            case INSERT:
//                // Handle insert
//                return database.insert(payload);
//            case UPDATE:
//                // Handle update
//                return database.update(payload);
//            case DELETE:
//                // Handle delete
//                return database.delete(payload);
//            case QUERY:
//                // Handle query
//                return database.query(payload);
            default:
                // Handle other cases or unknown operations
                return new byte[0];
        }
    }

    private byte[] combine(WireProtocol.Header header, byte[] jsonData) throws IOException {
        byte[] headerData = serializeHeader(header);
        byte[] combined = new byte[headerData.length + jsonData.length];
        System.arraycopy(headerData, 0, combined, 0, headerData.length);
        System.arraycopy(jsonData, 0, combined, headerData.length, jsonData.length);
        return combined;
    }

    private byte[] serializeHeader(WireProtocol.Header header) {
        // Serialize the header into bytes. This is a placeholder; you might need to implement the actual logic.
        return new byte[0];  // Placeholder
    }

    private WireProtocol.Header deserializeHeader(byte[] data) {
        // Deserialize the bytes into a WireProtocol.Header object. Placeholder for now.
        return null;  // Placeholder
    }

    public static void main(String[] args) throws IOException {
        DBServerWithEventLoop server = new DBServerWithEventLoop(8080, 10);  // For instance, 10 worker threads
        server.start();
    }
}