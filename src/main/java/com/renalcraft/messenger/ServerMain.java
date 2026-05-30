package com.renalcraft.messenger;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ServerMain extends WebSocketServer {

    private static final Set<WebSocket> connections = ConcurrentHashMap.newKeySet();

    public ServerMain(InetSocketAddress address) {
        super(address);
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        connections.add(conn);
        System.out.println("LOG: Подключено новое устройство. Всего онлайн: " + connections.size());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        connections.remove(conn);
        System.out.println("LOG: Устройство отключилось. Осталось онлайн: " + connections.size());
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        System.out.println("LOG: Получено сообщение -> " + message);
        for (WebSocket client : connections) {
            if (client != conn && client.isOpen()) {
                client.send(message);
            }
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("ERROR: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("LOG: WebSocket сервер успешно запущен и готов к работе!");
    }

    public static void main(String[] args) {
        String portEnv = System.getenv("PORT");
        int port = (portEnv != null) ? Integer.parseInt(portEnv) : 8080;

        ServerMain server = new ServerMain(new InetSocketAddress("0.0.0.0", port));
        server.start();
        System.out.println("LOG: Сервер слушает порт: " + port);
    }
}

