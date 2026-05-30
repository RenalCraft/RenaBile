package com.renalcraft.messenger.server;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.ConcurrentModificationException;
import java.util.concurrent.ConcurrentHashMap;

public class ServerMain extends WebSocketServer {

    // Безопасное хранилище для активных соединений
    private final Set<WebSocket> conns = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public ServerMain(int port) {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        conns.add(conn);
        System.out.println("Новое подключение: " + conn.getRemoteSocketAddress());
        System.out.flush(); // Мгновенный вывод в логи Render
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        conns.remove(conn);
        System.out.println("Соединение закрыто: " + conn.getRemoteSocketAddress() + " Причина: " + reason);
        System.out.flush(); // Мгновенный вывод в логи Render
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        System.out.println("Получено сообщение от " + conn.getRemoteSocketAddress() + ": " + message);
        System.out.flush(); // Мгновенный вывод в логи Render

        // Рассылаем сообщение всем остальным подключенным клиентам
        for (WebSocket sock : conns) {
            if (sock != conn && sock.isOpen()) {
                sock.send(message);
            }
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("Ошибка на сервере: " + ex.getMessage());
        System.err.flush(); // Мгновенный вывод ошибок в логи Render
        if (conn != null) {
            conns.remove(conn);
        }
    }

    @Override
    public void onStart() {
        System.out.println("WebSocket сервер успешно запущен на порту: " + getPort());
        System.out.flush(); // Мгновенный вывод в логи Render
    }

    public static void main(String[] args) {
        // Render автоматически назначает порт через переменную окружения PORT
        String portEnv = System.getenv("PORT");
        int port = 8080; // Стандартный порт, если переменная не задана
        if (portEnv != null) {
            port = Integer.parseInt(portEnv);
        }

        ServerMain server = new ServerMain(port);
        server.start();
    }
}
