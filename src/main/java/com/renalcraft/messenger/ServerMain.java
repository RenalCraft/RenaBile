package com.renalcraft.messenger.server;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ServerMain extends WebSocketServer {

    private final Map<WebSocket, String> activeConnections = new ConcurrentHashMap<>();
    private final Map<String, WebSocket> onlineUsers = new ConcurrentHashMap<>();
    private final Map<WebSocket, String> userCodes = new ConcurrentHashMap<>();

    private static final String DEFAULT_HOST = "dpg-d8drpdmk1jcs739b1t60-a.frankfurt-postgres.render.com";
    private static final String DB_NAME = "renabile_db";
    private static final String DB_USER = "renabile_db_user";
    private static final String DB_PASS = "Z6A4Hq5tNq639FAyWbJFaQjeUFQVYa78";

    public ServerMain(int port) { super(new InetSocketAddress(port)); }

    private Connection getConnection() throws SQLException {
        String envUrl = System.getenv("DATABASE_URL");
        if (envUrl != null && !envUrl.isEmpty()) return DriverManager.getConnection(envUrl);
        String url = "jdbc:postgresql://" + DEFAULT_HOST + ":5432/" + DB_NAME + "?ssl=true&sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory";
        return DriverManager.getConnection(url, DB_USER, DB_PASS);
    }

    private String computeHash(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : h) sb.append(String.format("%02x", b));
            return sb.toString().toLowerCase();
        } catch (Exception e) { return text.toLowerCase(); }
    }

    private boolean passwordMatches(String incomingPassword, String storedPassword) {
        if (incomingPassword == null || storedPassword == null) return false;

        String incomingLower = incomingPassword.toLowerCase();
        String storedLower = storedPassword.toLowerCase();

        // 1. Прямое совпадение
        if (incomingLower.equals(storedLower)) return true;

        // 2. Однократное хэширование
        String hashedOnce = computeHash(incomingPassword);
        if (hashedOnce.equals(storedLower)) return true;

        // 3. Двойное хэширование
        String doubleHashed = computeHash(hashedOnce);
        if (doubleHashed.equals(storedLower)) return true;

        return false;
    }

    private String hashPasswordForStorage(String pass) {
        if (pass.length() == 64 && pass.matches("^[0-9a-fA-F]+$")) {
            return pass.toLowerCase();
        }
        return computeHash(pass);
    }

    @Override public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("[ВХОД] Новое сетевое подключение со стороны: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        String user = activeConnections.remove(conn);
        userCodes.remove(conn);
        if (user != null) {
            System.out.println("[СЕТЬ] Пользователь " + user + " отключился.");
            onlineUsers.remove(user);
            broadcastStatusUpdate(user);
        }
    }

    @Override
    public void onMessage(WebSocket conn, String rawMessage) {
        try {
            String[] lines = rawMessage.split("\n");
            for (String line : lines) {
                if (line.trim().isEmpty()) continue;
                JSONObject packet = new JSONObject(line.trim());
                String type = packet.optString("type");
                JSONObject data = packet.optJSONObject("data");

                switch (type) {
                    case "REG": handleRegister(conn, data); break;
                    case "AUTH": handleAuth(conn, data); break;
                    case "MSG": handleMessage(conn, data); break;
                    case "ADD_FRIEND": handleAddFriend(conn, data); break;
                    case "GET_HISTORY": if (data != null) sendRoomHistory(conn, data.optString("room")); break;
                }
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    private void handleRegister(WebSocket conn, JSONObject data) throws Exception {
        String user = data.optString("username").trim();
        String pass = hashPasswordForStorage(data.optString("password").trim());
        String av = data.optString("avatar", "");

        try (Connection db = getConnection()) {
            PreparedStatement check = db.prepareStatement("SELECT username FROM users WHERE username = ?");
            check.setString(1, user);
            if (check.executeQuery().next()) {
                conn.send(new JSONObject().put("type", "ERROR").put("data", new JSONObject().put("message", "Логин занят")).toString());
                return;
            }

            String code = String.format("%04d", new Random().nextInt(10000));
            PreparedStatement ins = db.prepareStatement("INSERT INTO users (username, password, user_code, avatar_base64) VALUES (?, ?, ?, ?)");
            ins.setString(1, user); ins.setString(2, pass); ins.setString(3, code); ins.setString(4, av);
            ins.executeUpdate();

            authorize(conn, user, code);
        }
    }

    private void handleAuth(WebSocket conn, JSONObject data) throws Exception {
        String user = data.optString("username").trim();
        String pass = data.optString("password").trim();

        try (Connection db = getConnection()) {
            PreparedStatement ps = db.prepareStatement("SELECT password, user_code FROM users WHERE username = ?");
            ps.setString(1, user);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String storedPassword = rs.getString("password");
                if (passwordMatches(pass, storedPassword)) {
                    authorize(conn, user, rs.getString("user_code"));
                } else {
                    conn.send(new JSONObject().put("type", "ERROR").put("data", new JSONObject().put("message", "Неверный пароль")).toString());
                }
            } else {
                conn.send(new JSONObject().put("type", "ERROR").put("data", new JSONObject().put("message", "Неверные данные входа")).toString());
            }
        }
    }

    private void authorize(WebSocket conn, String user, String code) throws Exception {
        activeConnections.put(conn, user);
        onlineUsers.put(user, conn);
        userCodes.put(conn, code);

        conn.send(new JSONObject().put("type", "AUTH_OK").put("data", new JSONObject().put("username", user).put("code", code)).toString());
        sendRoomHistory(conn, "GLOBAL");
        sendFriendsList(conn, user);
        broadcastStatusUpdate(user);
    }

    private void handleAddFriend(WebSocket conn, JSONObject data) throws Exception {
        String myUsername = activeConnections.get(conn);
        if (myUsername == null || data == null) return;
        String friendCode = data.optString("code").trim();

        try (Connection db = getConnection()) {
            PreparedStatement findFriend = db.prepareStatement("SELECT username FROM users WHERE user_code = ?");
            findFriend.setString(1, friendCode);
            ResultSet rs = findFriend.executeQuery();

            if (!rs.next()) {
                conn.send(new JSONObject().put("type", "ERROR").put("data", new JSONObject().put("message", "Пользователь с таким кодом не найден")).toString());
                return;
            }
            String friendUsername = rs.getString("username");

            if (myUsername.equals(friendUsername)) {
                conn.send(new JSONObject().put("type", "ERROR").put("data", new JSONObject().put("message", "Нельзя добавить себя")).toString());
                return;
            }

            PreparedStatement ins1 = db.prepareStatement(
                    "INSERT INTO friends (username, friend_name) VALUES (?, ?) ON CONFLICT DO NOTHING");
            ins1.setString(1, myUsername);
            ins1.setString(2, friendUsername);
            ins1.executeUpdate();

            PreparedStatement ins2 = db.prepareStatement(
                    "INSERT INTO friends (username, friend_name) VALUES (?, ?) ON CONFLICT DO NOTHING");
            ins2.setString(1, friendUsername);
            ins2.setString(2, myUsername);
            ins2.executeUpdate();

            sendFriendsList(conn, myUsername);
            sendRoomHistory(conn, friendCode);

            WebSocket targetSocket = onlineUsers.get(friendUsername);
            if (targetSocket != null && targetSocket.isOpen()) {
                sendFriendsList(targetSocket, friendUsername);

                String myCode = userCodes.get(conn);
                if (myCode != null) {
                    sendRoomHistory(targetSocket, myCode);
                }
            }
        }
    }

    private void handleMessage(WebSocket conn, JSONObject data) throws Exception {
        String sender = activeConnections.get(conn);
        if (sender == null) return;
        String to = data.optString("to");
        String text = data.optString("text");
        String fromCode = data.optString("fromCode");

        try (Connection db = getConnection()) {
            PreparedStatement ps = db.prepareStatement("INSERT INTO messages (room, sender, text) VALUES (?, ?, ?)");
            ps.setString(1, to); ps.setString(2, sender); ps.setString(3, text);
            ps.executeUpdate();

            JSONObject msg = new JSONObject().put("type", "MSG").put("data", new JSONObject()
                    .put("senderName", sender).put("text", text).put("from", to.equals("GLOBAL") ? "GLOBAL" : fromCode));

            if (to.equals("GLOBAL")) {
                for (WebSocket ws : activeConnections.keySet()) {
                    if (ws.isOpen()) ws.send(msg.toString());
                }
            } else {
                conn.send(msg.toString());
                PreparedStatement find = db.prepareStatement("SELECT username FROM users WHERE user_code = ?");
                find.setString(1, to);
                ResultSet rs = find.executeQuery();
                if (rs.next()) {
                    WebSocket target = onlineUsers.get(rs.getString("username"));
                    if (target != null && target.isOpen()) {
                        JSONObject receiverMsg = new JSONObject().put("type", "MSG").put("data", new JSONObject()
                                .put("senderName", sender).put("text", text).put("from", fromCode));
                        target.send(receiverMsg.toString());
                    }
                }
            }
        }
    }

    private void sendRoomHistory(WebSocket conn, String room) throws Exception {
        try (Connection db = getConnection()) {
            PreparedStatement ps = db.prepareStatement("SELECT sender, text, to_char(timestamp, 'HH24:MI') as time FROM messages WHERE room = ? ORDER BY timestamp ASC");
            ps.setString(1, room);
            ResultSet rs = ps.executeQuery();
            JSONArray arr = new JSONArray();
            while (rs.next()) {
                arr.put(new JSONObject().put("sender", rs.getString("sender")).put("text", rs.getString("text")).put("time", rs.getString("time")));
            }
            conn.send(new JSONObject().put("type", "MSG_HISTORY").put("data", new JSONObject().put("room", room).put("history", arr)).toString());
        }
    }

    private void sendFriendsList(WebSocket conn, String user) throws Exception {
        try (Connection db = getConnection()) {
            PreparedStatement ps = db.prepareStatement("SELECT u.username, u.user_code, u.avatar_base64 FROM friends f JOIN users u ON f.friend_name = u.username WHERE f.username = ?");
            ps.setString(1, user);
            ResultSet rs = ps.executeQuery();
            JSONArray arr = new JSONArray();
            while (rs.next()) {
                String fName = rs.getString("username");
                arr.put(new JSONObject().put("username", fName).put("code", rs.getString("user_code"))
                        .put("avatar", rs.getString("avatar_base64") != null ? rs.getString("avatar_base64") : "")
                        .put("online", onlineUsers.containsKey(fName)));
            }
            conn.send(new JSONObject().put("type", "FRIENDS_LIST").put("data", new JSONObject().put("list", arr)).toString());
        }
    }

    private void broadcastStatusUpdate(String user) {
        try (Connection db = getConnection()) {
            PreparedStatement ps = db.prepareStatement("SELECT friend_name FROM friends WHERE username = ?");
            ps.setString(1, user);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                WebSocket ws = onlineUsers.get(rs.getString("friend_name"));
                if (ws != null && ws.isOpen()) sendFriendsList(ws, rs.getString("friend_name"));
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    @Override public void onError(WebSocket conn, Exception ex) {}
    @Override public void onStart() { System.out.println("[СЕРВЕР] Работает..."); }

    public static void main(String[] args) { new ServerMain(8080).start(); }
}
