package com.renalcraft.messenger.server;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ServerMain extends WebSocketServer {

    private final Map<WebSocket, String> activeSessions = new ConcurrentHashMap<>();
    private final Map<String, WebSocket> onlineUsers = new ConcurrentHashMap<>();

    // Константы подключения (Фолбэк, если Render не передал переменные)
    private static final String DEFAULT_HOST = "dpg-d8drpdmk1jcs739b1t60-a.frankfurt-postgres.render.com";
    private static final String DB_NAME = "renabile_db";
    private static final String DB_USER = "renabile_db_user";
    private static final String DB_PASS = "Z6A4Hq5tNq639FAyWbJFaQjeUFQVYa78";

    public ServerMain(int port) {
        super(new InetSocketAddress(port));
    }

    private Connection getFreshConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("[SERVER-DB] Драйвер PostgreSQL не найден!");
        }

        String jdbcUrl;
        String envUrl = System.getenv("DATABASE_URL");

        if (envUrl != null && !envUrl.isEmpty()) {
            // Если Render передал нам нормальную строку, очищаем её от параметров
            if (envUrl.contains("?")) {
                envUrl = envUrl.substring(0, envUrl.indexOf("?"));
            }
            if (envUrl.startsWith("jdbc:")) {
                envUrl = envUrl.substring(5);
            }
            if (!envUrl.contains(":5432") && envUrl.contains(".com/")) {
                envUrl = envUrl.replace(".com/", ".com:5432/");
            }
            jdbcUrl = "jdbc:" + envUrl;
        } else {
            // Если переменной нет, собираем чистый URL из констант вручную
            jdbcUrl = "jdbc:postgresql://" + DEFAULT_HOST + ":5432/" + DB_NAME;
        }

        // Профессиональная настройка свойств подключения через Properties
        Properties props = new Properties();
        props.setProperty("user", DB_USER);
        props.setProperty("password", DB_PASS);
        props.setProperty("ssl", "true");
        props.setProperty("sslmode", "require");
        props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory"); // Игнорируем капризы сертификатов Render
        props.setProperty("connectTimeout", "10"); // Чтобы сервер не зависал бесконечно при ошибке сети

        return DriverManager.getConnection(jdbcUrl, props);
    }

    @Override public void onOpen(WebSocket conn, ClientHandshake handshake) {}

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        String username = activeSessions.remove(conn);
        if (username != null) {
            onlineUsers.remove(username);
            broadcastFriendListUpdate(username);
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        try {
            JSONObject json = new JSONObject(message);
            String type = json.getString("type");

            switch (type) {
                case "REG": handleRegister(conn, json); break;
                case "AUTH": handleAuth(conn, json); break;
                case "ADD_FRIEND": handleAddFriend(conn, json); break;
                case "MSG": handleMessage(conn, json); break;
                case "REFRESH_FRIENDS":
                    String me = activeSessions.get(conn);
                    if(me != null) sendFriendList(me, conn);
                    break;
                case "PING":
                    conn.send(new JSONObject().put("type", "PONG").toString());
                    break;
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    private void handleRegister(WebSocket conn, JSONObject json) {
        String user = json.getString("username").trim();
        String pass = json.getString("password").trim();

        try (Connection db = getFreshConnection()) {
            PreparedStatement check = db.prepareStatement("SELECT 1 FROM users WHERE username = ?");
            check.setString(1, user);
            if (check.executeQuery().next()) {
                sendResponse(conn, "ERROR", "Этот логин уже занят другим пользователем!");
                return;
            }

            String code; Random rand = new Random();
            PreparedStatement checkCode = db.prepareStatement("SELECT 1 FROM users WHERE user_code = ?");
            do {
                code = String.format("%04d", rand.nextInt(10000));
                checkCode.setString(1, code);
            } while (checkCode.executeQuery().next());

            PreparedStatement insert = db.prepareStatement("INSERT INTO users (username, password, user_code, avatar_base64) VALUES (?, ?, ?, '')");
            insert.setString(1, user); insert.setString(2, pass); insert.setString(3, code);
            insert.executeUpdate();

            JSONObject data = new JSONObject();
            data.put("code", code);
            sendResponse(conn, "REG_OK", data);
        } catch (Exception e) { sendResponse(conn, "ERROR", e.getMessage()); }
    }

    private void handleAuth(WebSocket conn, JSONObject json) {
        String user = json.getString("username").trim();
        String pass = json.getString("password").trim();

        try (Connection db = getFreshConnection()) {
            PreparedStatement query = db.prepareStatement("SELECT user_code FROM users WHERE username = ? AND password = ?");
            query.setString(1, user); query.setString(2, pass);
            ResultSet rs = query.executeQuery();

            if (!rs.next()) {
                sendResponse(conn, "ERROR", "Неверный логин или пароль!");
                return;
            }

            String code = rs.getString("user_code");
            activeSessions.put(conn, user);
            onlineUsers.put(user, conn);

            JSONObject data = new JSONObject();
            data.put("code", code);
            sendResponse(conn, "AUTH_OK", data);

            sendFriendList(user, conn);
            broadcastFriendListUpdate(user);
        } catch (Exception e) { sendResponse(conn, "ERROR", e.getMessage()); }
    }

    private void handleAddFriend(WebSocket conn, JSONObject json) {
        String me = activeSessions.get(conn);
        if (me == null) return;
        String targetCode = json.getString("code").trim();

        try (Connection db = getFreshConnection()) {
            PreparedStatement find = db.prepareStatement("SELECT username FROM users WHERE user_code = ?");
            find.setString(1, targetCode);
            ResultSet rs = find.executeQuery();

            if (!rs.next()) {
                sendResponse(conn, "ERROR", "Пользователь не найден!");
                return;
            }
            String friendName = rs.getString("username");
            if (friendName.equals(me)) return;

            PreparedStatement add1 = db.prepareStatement("INSERT INTO friends VALUES (?, ?) ON CONFLICT DO NOTHING");
            add1.setString(1, me); add1.setString(2, friendName); add1.executeUpdate();

            PreparedStatement add2 = db.prepareStatement("INSERT INTO friends VALUES (?, ?) ON CONFLICT DO NOTHING");
            add2.setString(1, friendName); add2.setString(2, me); add2.executeUpdate();

            sendFriendList(me, conn);
            WebSocket friendConn = onlineUsers.get(friendName);
            if (friendConn != null) sendFriendList(friendName, friendConn);
        } catch (Exception e) { e.printStackTrace(); }
    }

    private void handleMessage(WebSocket conn, JSONObject json) {
        String me = activeSessions.get(conn);
        if (me == null) return;

        String toTarget = json.getString("to");
        String text = json.getString("text");
        String senderCode = json.getString("fromCode");

        JSONObject msgData = new JSONObject();
        msgData.put("from", toTarget.equals("GLOBAL") ? "GLOBAL" : senderCode);
        msgData.put("senderName", me);
        msgData.put("text", text);

        if (toTarget.equals("GLOBAL")) {
            for (Map.Entry<WebSocket, String> session : activeSessions.entrySet()) {
                if (session.getKey() != conn && session.getKey().isOpen()) {
                    sendResponse(session.getKey(), "MSG", msgData);
                }
            }
        } else {
            try (Connection db = getFreshConnection()) {
                PreparedStatement find = db.prepareStatement("SELECT username FROM users WHERE user_code = ?");
                find.setString(1, toTarget);
                ResultSet rs = find.executeQuery();
                if (rs.next()) {
                    WebSocket targetConn = onlineUsers.get(rs.getString("username"));
                    if (targetConn != null && targetConn.isOpen()) {
                        sendResponse(targetConn, "MSG", msgData);
                    }
                }
            } catch (Exception e) { e.printStackTrace(); }
        }
    }

    private void sendFriendList(String username, WebSocket conn) {
        if (conn == null || !conn.isOpen()) return;
        try (Connection db = getFreshConnection()) {
            PreparedStatement query = db.prepareStatement(
                    "SELECT u.username, u.user_code FROM friends f " +
                    "JOIN users u ON f.friend_name = u.username WHERE f.username = ?");
            query.setString(1, username);
            ResultSet rs = query.executeQuery();

            JSONArray array = new JSONArray();
            while (rs.next()) {
                JSONObject fJson = new JSONObject();
                String fName = rs.getString("username");
                fJson.put("username", fName);
                fJson.put("code", rs.getString("user_code"));
                fJson.put("online", onlineUsers.containsKey(fName));
                array.put(fJson);
            }
            sendResponse(conn, "FRIENDS_LIST", new JSONObject().put("list", array));
        } catch (Exception e) { e.printStackTrace(); }
    }

    private void broadcastFriendListUpdate(String username) {
        try (Connection db = getFreshConnection()) {
            PreparedStatement query = db.prepareStatement("SELECT friend_name FROM friends WHERE username = ?");
            query.setString(1, username);
            ResultSet rs = query.executeQuery();
            while (rs.next()) {
                String friendName = rs.getString("friend_name");
                WebSocket friendConn = onlineUsers.get(friendName);
                if (friendConn != null) sendFriendList(friendName, friendConn);
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    private void sendResponse(WebSocket conn, String type, Object data) {
        JSONObject resp = new JSONObject(); resp.put("type", type); resp.put("data", data);
        if (conn != null && conn.isOpen()) conn.send(resp.toString());
    }

    @Override public void onError(WebSocket conn, Exception ex) {}
    @Override public void onStart() { System.out.println("Java Core WebSocket Server онлайн."); }

    public static void main(String[] args) {
        int port = 8080;
        String portEnv = System.getenv("PORT");
        if (portEnv != null) port = Integer.parseInt(portEnv);
        new ServerMain(port).start();
    }
}
