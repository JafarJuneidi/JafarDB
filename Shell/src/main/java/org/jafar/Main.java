package org.jafar;

import java.io.DataInputStream;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        String serverAddress = "localhost";
        int boostrapPort = 8888;
        int receivedPort = 0;

        try (Socket socket = new Socket(serverAddress, boostrapPort);
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            receivedPort = in.readInt();
            System.out.println("Received port from bootstrap: " + receivedPort);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        Shell shell = new Shell(serverAddress, receivedPort);
        shell.start();
    }
}
