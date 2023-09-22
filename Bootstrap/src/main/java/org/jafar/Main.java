package org.jafar;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    private final String[] nodes = {"node1", "node2"};
    private static final int[] ports = {9001, 9002};
    private static int roundRobinIndex = 0;
    private static final int BOOTSTRAP_PORT = 8888;

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(BOOTSTRAP_PORT)) {
            System.out.println("Bootstrap Node started on port: " + BOOTSTRAP_PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from client: " + clientSocket.getInetAddress());

                try (DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {
                    int port = ports[roundRobinIndex];
                    out.writeInt(port);
                    System.out.println("Sent port: " + port);
                    roundRobinIndex = (roundRobinIndex + 1) % ports.length;
                }

                clientSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}