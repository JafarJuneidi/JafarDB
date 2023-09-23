package org.jafar;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class WireProtocol {
    public static class Header {
        private OperationType operationType;
        private int payloadLength;
        private boolean isBroadcast;

        public Header(OperationType operationType, int payloadLength) {
            this(operationType, payloadLength, false);
        }

        public Header(OperationType operationType, int payloadLength, boolean isBroadcast) {
            this.operationType = operationType;
            this.payloadLength = payloadLength;
            this.isBroadcast = isBroadcast;
        }

        public OperationType getOperationType() {
            return operationType;
        }
        public int getPayloadLength() {
            return payloadLength;
        }

        public byte[] serialize() {
            ByteBuffer buffer = ByteBuffer.wrap(allocateHeaderSpace());
            buffer.putInt(operationType.ordinal());
            buffer.putInt(payloadLength);
            buffer.put((byte) (isBroadcast ? 1 : 0));
            return buffer.array();
        }

        public static Header deserialize(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);

            OperationType operationType = OperationType.values()[buffer.getInt()];
            int bodyLength = buffer.getInt();
            boolean isBroadcast = buffer.get() == 1;

            return new Header(operationType, bodyLength, isBroadcast);
        }

        public static byte[] allocateHeaderSpace() {
            return new byte[4 + 4 + 1];
        }
    }

    public static class Message {
        private final Header header;
        private final byte[] payload;

        public byte[] getPayload() { return this.payload; }

        public OperationType getOperationType() { return header.operationType; }
        public boolean getIsBroadcast() { return header.isBroadcast; }
        public void setBroadcast(boolean isBroadcast) { this.header.isBroadcast = isBroadcast; }

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
        byte[] responseHeaderData = Header.allocateHeaderSpace();
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
        SHOW_DATABASES,
        DELETE_COLLECTION,
        DELETE_DATABASE
    }
}