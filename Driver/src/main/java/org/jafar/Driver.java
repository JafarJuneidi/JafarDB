package org.jafar;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    public <T> Response insert(T obj) throws IOException {
        return sendToDb(obj, WireProtocol.OperationType.INSERT);
    }

    public <T> Response update(T obj) throws IOException {
        return sendToDb(obj, WireProtocol.OperationType.UPDATE);
    }

    public <T> Response delete(T obj) throws IOException {
        return sendToDb(obj, WireProtocol.OperationType.DELETE);
    }

    public <T> Response get(Class<T> clazz) throws IOException {
        return sendToDb(null, WireProtocol.OperationType.QUERY);
    }

    private <T> Response sendToDb(T obj, WireProtocol.OperationType operationType) throws IOException {
        byte[] jsonData = obj != null ? objectMapper.writeValueAsBytes(obj) : new byte[0];
        WireProtocol.Header header = new WireProtocol.Header(operationType, jsonData.length);

        byte[] message = combine(header, jsonData);
        out.write(message);

        return receiveResponse();
    }

    private byte[] combine(WireProtocol.Header header, byte[] jsonData) throws IOException {
        byte[] headerData = serializeHeader(header);
        byte[] combined = new byte[headerData.length + jsonData.length];
        System.arraycopy(headerData, 0, combined, 0, headerData.length);
        System.arraycopy(jsonData, 0, combined, headerData.length, jsonData.length);
        return combined;
    }

    private Response receiveResponse() throws IOException {
        // For the sake of demonstration, assuming that the response is also a WireProtocol message.
        byte[] responseHeaderData = new byte[2]; /* size of header (short) */
        in.read(responseHeaderData);
        WireProtocol.Header responseHeader = deserializeHeader(responseHeaderData);

        byte[] payload = new byte[responseHeader.getPayloadLength()];
        in.read(payload);

        return new Response(responseHeader, payload);
    }

    private byte[] serializeHeader(WireProtocol.Header header) {
        // Serialize the header into bytes. This is a placeholder; you might need to implement the actual logic.
        return new byte[0];  // Placeholder
    }

    private WireProtocol.Header deserializeHeader(byte[] data) {
        // Deserialize the bytes into a WireProtocol.Header object. Placeholder for now.
        return null;  // Placeholder
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