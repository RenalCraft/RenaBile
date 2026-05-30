import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ServerMain extends WebSocketServer {

    private final Set<WebSocket> conns = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public ServerMain(int port) {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        conns.add(conn);
        System.out.println("Новое подключение: " + conn.getRemoteSocketAddress());
        System.out.flush();
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        conns.remove(conn);
        System.out.println("Соединение закрыто: " + conn.getRemoteSocketAddress() + " Причина: " + reason);
        System.out.flush();
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        System.out.println("Получено сообщение от " + conn.getRemoteSocketAddress() + ": " + message);
        System.out.flush();

        for (WebSocket sock : conns) {
            if (sock != conn && sock.isOpen()) {
                sock.send(message);
            }
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("Ошибка на сервере: " + ex.getMessage());
        System.err.flush();
        if (conn != null) {
            conns.remove(conn);
        }
    }

    @Override
    public void onStart() {
        System.out.println("WebSocket сервер успешно запущен на порту: " + getPort());
        System.out.flush();
    }

    public static void main(String[] args) {
        String portEnv = System.getenv("PORT");
        int port = 8080;
        if (portEnv != null) {
            port = Integer.parseInt(portEnv);
        }

        ServerMain server = new ServerMain(port);
        server.start();
    }
}
