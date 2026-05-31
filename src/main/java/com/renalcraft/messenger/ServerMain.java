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

    // Жесткая запаска на случай, если системная переменная вдруг пропадет
    private static final String DEFAULT_HOST = "dpg-d8drpdmk1jcs739b1t60-a";
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

        String envUrl = System.getenv("DATABASE_URL");
        String jdbcUrl = null;
        String user = null;
        String pass = null;

        if (envUrl != null && !envUrl.isEmpty()) {
            try {
                String cleanUrl = envUrl.replace("postgresql://", "").replace("jdbc:postgresql://", "");
                String[] userInfoAndRest = cleanUrl.split("@");
                if (userInfoAndRest.length == 2) {
                    String[] credentials = userInfoAndRest[0].split(":");
                    user = credentials[0];
                    pass = credentials[1];

                    String hostAndDb = userInfoAndRest[1];
                    if (hostAndDb.contains(".frankfurt-postgres.render.com")) {
                        hostAndDb = hostAndDb.replace(".frankfurt-postgres.render.com", ":5432");
                    } else if (!hostAndDb.contains(":5432")) {
                        hostAndDb = hostAndDb.replace("/", ":5432/");
                    }
                    jdbcUrl = "jdbc:postgresql://" + hostAndDb;
                }
            } catch (Exception e) {
                System.err.println("[SERVER-DB] Ошибка автопарсинга DATABASE_URL, включаем запаску: " + e.getMessage());
            }
        }

        if (jdbcUrl == null) {
            jdbcUrl = "jdbc:postgresql://" + DEFAULT_HOST + ":5432/" + DB_NAME;
            user = DB_USER;
            pass = DB_PASS;
        }

        if (!jdbcUrl.contains("?")) {
            jdbcUrl += "?ssl=true&sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory&allowEncodingChanges=true&connectTimeout=10";
        }

        return DriverManager.getConnection(jdbcUrl, user, pass);
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String ip = conn.getRemoteSocketAddress().getAddress().getHostAddress();
        System.out.println("[СЕТЬ] Новое подключение! IP: " + ip);
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        String username = activeSessions.remove(conn);
        if (username != null) {
            onlineUsers.remove(username);
            System.out.println("[СЕТЬ] Пользователь " + username + " отключился.");
            broadcastFriendListUpdate(username);
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // Очищаем от переносов строк \n, которые шлет Андроид для проталкивания буфера
        String cleanMessage = message.trim();
        System.out.println("[СЕТЬ] Получено сообщение: " + cleanMessage);

        try {
            JSONObject json = new JSONObject(cleanMessage);
            String rawType = json.optString("type", "").toUpperCase();

            if ("PING".equals(rawType)) {
                conn.send(new JSONObject().put("type", "PONG").toString());
                return;
            }

            // Унифицируем типы пакетов (и для ПК, и для Андроида)
            String type = rawType;
            if ("LOGIN".equals(rawType)) type = "AUTH";
            if ("REGISTER".equals(rawType)) type = "REG";
            if ("MESSAGE".equals(rawType)) type = "MSG";

            // КРОСС-ПЛАТФОРМЕННЫЙ ХАК: Вытаскиваем "data"
            JSONObject data;
            if (json.has("data")) {
                data = json.getJSONObject("data");
            } else {
                // Если пришел плоский JSON, используем сам корень
                data = json;
            }

            switch (type) {
                case "REG":
                    handleRegister(conn, data);
                    break;
                case "AUTH":
                    handleAuth(conn, data);
                    break;
                case "ADD_FRIEND":
                    handleAddFriend(conn, data);
                    break;
                case "MSG":
                    handleMessage(conn, data);
                    break;
                case "REFRESH_FRIENDS":
                    String me = activeSessions.get(conn);
                    if (me != null) sendFriendList(me, conn);
                    break;
                default:
                    System.out.println("[СЕТЬ] Неизвестный тип пакета: " + type);
                    break;
            }
        } catch (Exception e) {
            System.err.println("[ОШИБКА ОБРАБОТКИ СООБЩЕНИЯ] " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleRegister(WebSocket conn, JSONObject data) {
        String user = data.getString("username").trim();
        String pass = data.getString("password").trim();
        String ip = conn.getRemoteSocketAddress().getAddress().getHostAddress();

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

            System.out.println("[БАЗА] Зарегистрирован новый юзер: " + user + " | Код: " + code + " | IP: " + ip);

            JSONObject respData = new JSONObject();
            respData.put("code", code);

            // Отправляем ответ, дублируя REG_OK и auth_success для совместимости
            sendResponse(conn, "REG_OK", respData);
            sendResponse(conn, "auth_success", respData);
        } catch (Exception e) {
            System.err.println("[ОШИБКА REG] " + e.getMessage());
            sendResponse(conn, "ERROR", "Ошибка регистрации на сервере: " + e.getMessage());
        }
    }

    private void handleAuth(WebSocket conn, JSONObject data) {
        String user = data.getString("username").trim();
        String pass = data.getString("password").trim();
        String ip = conn.getRemoteSocketAddress().getAddress().getHostAddress();

        try (Connection db = getFreshConnection()) {
            PreparedStatement query = db.prepareStatement("SELECT user_code FROM users WHERE username = ? AND password = ?");
            query.setString(1, user); query.setString(2, pass);
            ResultSet rs = query.executeQuery();

            if (!rs.next()) {
                // Дублируем ошибку для обоих типов клиентов
                sendResponse(conn, "ERROR", "Неверный логин или пароль!");
                sendResponse(conn, "auth_fail", new JSONObject().put("message", "Неверный логин или пароль!"));
                return;
            }

            String code = rs.getString("user_code");
            activeSessions.put(conn, user);
            onlineUsers.put(user, conn);

            System.out.println("[БАЗА] Юзер успешно авторизован: " + user + " | Код: " + code + " | IP: " + ip);

            JSONObject respData = new JSONObject();
            respData.put("code", code);

            // Отправляем успешный статус обоим видам клиентов (ПК и Андроид)
            sendResponse(conn, "AUTH_OK", respData);
            sendResponse(conn, "auth_success", respData);

            sendFriendList(user, conn);
            broadcastFriendListUpdate(user);
        } catch (Exception e) {
            System.err.println("[ОШИБКА AUTH] " + e.getMessage());
            sendResponse(conn, "ERROR", "Ошибка авторизации на сервере: " + e.getMessage());
        }
    }

    private void handleAddFriend(WebSocket conn, JSONObject data) {
        String me = activeSessions.get(conn);
        if (me == null) return;

        // Поддержка поиска как по коду (0101), так и по имени ("friend")
        String target = data.has("code") ? data.getString("code").trim() : data.optString("friend", "").trim();

        try (Connection db = getFreshConnection()) {
            PreparedStatement find = db.prepareStatement("SELECT username FROM users WHERE user_code = ? OR username = ?");
            find.setString(1, target); find.setString(2, target);
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

    private void handleMessage(WebSocket conn, JSONObject data) {
        String me = activeSessions.get(conn);
        if (me == null) return;

        // Корректируем поля для совместимости
        String toTarget = data.has("to") ? data.getString("to") : data.optString("chat", "GLOBAL");
        String text = data.has("text") ? data.getString("text") : data.optString("message", "");
        String senderCode = data.optString("fromCode", "GLOBAL");

        JSONObject msgData = new JSONObject();
        msgData.put("from", toTarget.equals("GLOBAL") || toTarget.equals("Общий чат") ? "GLOBAL" : senderCode);
        msgData.put("senderName", me);
        msgData.put("text", text);

        // Поля под Андроид клиент
        msgData.put("message", text);

        if (toTarget.equals("GLOBAL") || toTarget.equals("Общий чат")) {
            for (Map.Entry<WebSocket, String> session : activeSessions.entrySet()) {
                if (session.getKey() != conn && session.getKey().isOpen()) {
                    sendResponse(session.getKey(), "MSG", msgData);
                    sendResponse(session.getKey(), "message", msgData); // Для Андроида
                }
            }
        } else {
            try (Connection db = getFreshConnection()) {
                PreparedStatement find = db.prepareStatement("SELECT username FROM users WHERE user_code = ? OR username = ?");
                find.setString(1, toTarget); find.setString(2, toTarget);
                ResultSet rs = find.executeQuery();
                if (rs.next()) {
                    WebSocket targetConn = onlineUsers.get(rs.getString("username"));
                    if (targetConn != null && targetConn.isOpen()) {
                        sendResponse(targetConn, "MSG", msgData);
                        sendResponse(targetConn, "message", msgData); // Для Андроида
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

            // Совместимость со списком комнат/чатов для Андроида
            sendResponse(conn, "update_chats", new JSONObject().put("chats", array));
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

    @Override
    public void onStart() {
        System.out.println("Java Кроссплатформенный WebSocket Server онлайн. Ожидание ПК и Мобилок...");
    }

    public static void main(String[] args) {
        int port = 8080;
        String portEnv = System.getenv("PORT");
        if (portEnv != null) port = Integer.parseInt(portEnv);
        new ServerMain(port).start();
    }
}
