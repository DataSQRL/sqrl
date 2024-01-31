package com.datasqrl.cmd;

import java.io.*;
import java.net.*;

/**
 * A simple port proxy for the kafka embedded engine because ports are not assignable
 */
public class PortForwarder {

  private final int targetPort;
  private final ServerSocket serverSocket;

  public PortForwarder(int listenPort, int targetPort) throws IOException {
    this.serverSocket = new ServerSocket(listenPort);
    this.targetPort = targetPort;
  }

  public void start() {
    System.out.println("Starting port forwarder on port " + serverSocket.getLocalPort());
    while (true) {
      try {
        Socket clientSocket = serverSocket.accept();
        new Thread(new ClientHandler(clientSocket, targetPort)).start();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private static class ClientHandler implements Runnable {

    private final Socket clientSocket;
    private final int targetPort;

    public ClientHandler(Socket socket, int targetPort) {
      this.clientSocket = socket;
      this.targetPort = targetPort;
    }

    @Override
    public void run() {
      try (Socket targetSocket = new Socket("localhost", targetPort);
          InputStream clientInput = clientSocket.getInputStream();
          OutputStream clientOutput = clientSocket.getOutputStream();
          InputStream targetInput = targetSocket.getInputStream();
          OutputStream targetOutput = targetSocket.getOutputStream()) {

        Thread clientToTarget = new Thread(() -> transferData(clientInput, targetOutput));
        Thread targetToClient = new Thread(() -> transferData(targetInput, clientOutput));

        clientToTarget.start();
        targetToClient.start();

        clientToTarget.join();
        targetToClient.join();
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      } finally {
        try {
          clientSocket.close();
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    }

    private void transferData(InputStream in, OutputStream out) {
      try {
        byte[] buffer = new byte[4096];
        int read;
        while ((read = in.read(buffer)) != -1) {
          out.write(buffer, 0, read);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
