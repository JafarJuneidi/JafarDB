package org.jafar;

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
    }

    public static class Message {
        private Header header;
        private byte[] payload;

        public Message(Header header, byte[] payload) {
            this.header = header;
            this.payload = payload;
        }

        public Header getHeader() {
            return header;
        }

        public void setHeader(Header header) {
            this.header = header;
        }

        public byte[] getPayload() {
            return payload;
        }

        public void setPayload(byte[] payload) {
            this.payload = payload;
        }
    }

    public enum OperationType {
        INSERT,
        UPDATE,
        DELETE,
        QUERY,
        RESPONSE
        // ... any other operation types you might need
    }
}