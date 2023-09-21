package org.jafar;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class WireProtocol {
    public static class Header {
        private OperationType operationType;
        private int payloadLength;

        public Header(OperationType operationType, int payloadLength) {
            this.operationType = operationType;
            this.payloadLength = payloadLength;
        }

        public OperationType getOperationType() {
            return operationType;
        }

        public void setOperationType(OperationType operationType) {
            this.operationType = operationType;
        }

        public int getPayloadLength() {
            return payloadLength;
        }

        public void setPayloadLength(int payloadLength) {
            this.payloadLength = payloadLength;
        }

        public byte[] serialize() {
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4); // 4 bytes for the int representation of enum, 4 bytes for bodyLength
            buffer.putInt(operationType.ordinal());
            buffer.putInt(payloadLength);
            return buffer.array();
        }

        public static Header deserialize(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);

            OperationType operationType = OperationType.values()[buffer.getInt()];
            int bodyLength = buffer.getInt();

            return new Header(operationType, bodyLength);
        }
    }

    public static class Message {
        private final Header header;
        private final byte[] payload;

        public byte[] getPayload() { return this.payload; }

        public OperationType getOperationType() { return header.operationType; }

        public Message(Header header, byte[] payload) {
            this.header = header;
            this.payload = payload;
        }

        public byte[] serialize() {
            byte[] headerBytes = header.serialize();
            byte[] combined = new byte[headerBytes.length + payload.length];
            System.arraycopy(headerBytes, 0, combined, 0, headerBytes.length);
            System.arraycopy(payload, 0, combined, headerBytes.length, payload.length);
            return combined;
        }
    }

    public static Message createMessage(OperationType operationType, byte[] objectNode) {
        Header header = new Header(operationType, objectNode.length);
        return new Message(header, objectNode);
    }

    public static Message createMessage(InputStream in) throws IOException {
        byte[] responseHeaderData = new byte[4 + 4];
        in.read(responseHeaderData);
        WireProtocol.Header responseHeader = WireProtocol.Header.deserialize(responseHeaderData);

        byte[] payload = new byte[responseHeader.getPayloadLength()];
        in.read(payload);

        return new Message(responseHeader, payload);
    }

    public enum OperationType {
        CREATE_DB,
        CREATE_COLLECTION,
        INSERT,
        UPDATE,
        DELETE,
        QUERY,
        RESPONSE,
        SHOW_COLLECTIONS,
        DELETE_COLLECTION
    }
}