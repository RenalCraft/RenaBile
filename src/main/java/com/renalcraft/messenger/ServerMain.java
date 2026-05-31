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
import java.util.concurrent.CopyOnWriteArrayList;

public class ServerMain extends WebSocketServer {

    // Твой External Database URL для тестов сервера прямо с ПК
    private static final String DEFAULT_DB_URL = "postgresql://renabile_db_user:Z6A4Hq5tNq639FAyWbJFaQjeUFQVYa78@dpg-d8drpdmk1jcs739b1t60-a.frankfurt-postgres.render.com/renabile_db";

    static class ChatMessage {
        String from;
        String to;
        String message;

        ChatMessage(String from, String to, String message) {
            this.from = from;
            this.to = to;
            this.message = message;
        }
    }

    // Оперативная память для активных сессий
    private final Map<WebSocket, String> activeSessions = new ConcurrentHashMap<>();
    private final Map<String, WebSocket> onlineUsers = new ConcurrentHashMap<>();
    private final List<ChatMessage> globalHistory = new CopyOnWriteArrayList<>();

    public ServerMain(int port) {
        super(new InetSocketAddress(port));
        initDatabase();
    }

    // Подключение к PostgreSQL с авто-заменой протокола под JDBC
    private Connection getConnection() throws SQLException {
        String dbUrl = System.getenv("DATABASE_URL"); // Если запущено на Render
        if (dbUrl == null || dbUrl.isEmpty()) {
            dbUrl = DEFAULT_DB_URL; // Если запускаешь на ПК
        }

        // Пересобираем строку под стандарт JDBC драйвера
        if (dbUrl.startsWith("postgres://")) {
            dbUrl = dbUrl.replace("postgres://", "jdbc:postgresql://");
        } else if (dbUrl.startsWith("postgresql://")) {
            dbUrl = dbUrl.replace("postgresql://", "jdbc:postgresql://");
        }

        return DriverManager.getConnection(dbUrl);
    }

    // Создание таблиц в базе, если их ещё нет
    private void initDatabase() {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Таблица аккаунтов
            stmt.execute("CREATE TABLE IF NOT EXISTS users (" +
                    "username VARCHAR(50) PRIMARY KEY, " +
                    "password VARCHAR(100) NOT NULL, " +
                    "user_code VARCHAR(10) UNIQUE NOT NULL, " +
                    "avatar_base64 TEXT DEFAULT '')");

            // Таблица связей друзей
            stmt.execute("CREATE TABLE IF NOT EXISTS friends (" +
                    "username VARCHAR(50), " +
                    "friend_name VARCHAR(50), " +
                    "PRIMARY KEY (username, friend_name))");

            System.out.println("[DB] База данных PostgreSQL успешно подключена и проверена.");
        } catch (Exception e) {
            System.err.println("[DB Error] Критическая ошибка инициализации таблиц: " + e.getMessage());
        }
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("Новое сетевое подключение: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        String username = activeSessions.remove(conn);
        if (username != null) {
            onlineUsers.remove(username);
            System.out.println("Пользователь " + username + " ушел в оффлайн.");
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
                case "SET_AVATAR": handleSetAvatar(conn, json); break;
            }
        } catch (Exception e) {
            sendError(conn, "Ошибка обработки пакета: " + e.getMessage());
        }
    }

    private void handleRegister(WebSocket conn, JSONObject json) {
        String user = json.getString("username").trim();
        String pass = json.getString("password").trim();

        try (Connection db = getConnection()) {
            PreparedStatement check = db.prepareStatement("SELECT 1 FROM users WHERE username = ?");
            check.setString(1, user);
            if (check.executeQuery().next()) {
                sendResponse(conn, "REG_FAIL", "Этот логин уже занят!");
                return;
            }

            // Генерируем уникальный тег #XXXX
            String code;
            Random rand = new Random();
            PreparedStatement checkCode = db.prepareStatement("SELECT 1 FROM users WHERE user_code = ?");
            do {
                code = "#" + (1000 + rand.nextInt(9000));
                checkCode.setString(1, code);
            } while (checkCode.executeQuery().next());

            PreparedStatement insert = db.prepareStatement(
                    "INSERT INTO users (username, password, user_code) VALUES (?, ?, ?)");
            insert.setString(1, user);
            insert.setString(2, pass);
            insert.setString(3, code);
            insert.executeUpdate();

            System.out.println("[DB] Зарегистрирован: " + user + " (" + code + ")");
            sendResponse(conn, "REG_OK", new JSONObject().put("code", code));
        } catch (Exception e) {
            sendError(conn, "Ошибка регистрации в БД: " + e.getMessage());
        }
    }

    private void handleAuth(WebSocket conn, JSONObject json) {
        String user = json.getString("username").trim();
        String pass = json.getString("password").trim();

        try (Connection db = getConnection()) {
            PreparedStatement query = db.prepareStatement(
                    "SELECT user_code, avatar_base64 FROM users WHERE username = ? AND password = ?");
            query.setString(1, user);
            query.setString(2, pass);
            ResultSet rs = query.executeQuery();

            if (!rs.next()) {
                sendResponse(conn, "AUTH_FAIL", "Неверный логин или пароль!");
                return;
            }

            String code = rs.getString("user_code");
            String avatar = rs.getString("avatar_base64");

            activeSessions.put(conn, user);
            onlineUsers.put(user, conn);

            System.out.println("[DB] Успешный вход: " + user);

            JSONObject data = new JSONObject();
            data.put("code", code);
            data.put("avatar", avatar != null ? avatar : "");
            sendResponse(conn, "AUTH_OK", data);

            sendFriendList(user, conn);
            sendChatHistory(user, conn);
            broadcastFriendListUpdate(user);
        } catch (Exception e) {
            sendError(conn, "Ошибка авторизации в БД: " + e.getMessage());
        }
    }

    private void handleAddFriend(WebSocket conn, JSONObject json) {
        String me = activeSessions.get(conn);
        if (me == null) return;

        String targetCode = json.getString("code").trim();

        try (Connection db = getConnection()) {
            PreparedStatement find = db.prepareStatement("SELECT username FROM users WHERE user_code = ?");
            find.setString(1, targetCode);
            ResultSet rs = find.executeQuery();

            if (!rs.next()) {
                sendResponse(conn, "ADD_FRIEND_FAIL", "Пользователь с таким кодом не найден!");
                return;
            }

            String friendName = rs.getString("username");

            if (friendName.equals(me)) {
                sendResponse(conn, "ADD_FRIEND_FAIL", "Нельзя добавить самого себя!");
                return;
            }

            PreparedStatement check = db.prepareStatement(
                    "SELECT 1 FROM friends WHERE username = ? AND friend_name = ?");
            check.setString(1, me);
            check.setString(2, friendName);
            if (check.executeQuery().next()) {
                sendResponse(conn, "ADD_FRIEND_FAIL", "Этот пользователь уже у вас в друзьях!");
                return;
            }

            // Зеркальное добавление связей
            PreparedStatement add1 = db.prepareStatement("INSERT INTO friends VALUES (?, ?)");
            add1.setString(1, me);
            add1.setString(2, friendName);
            add1.executeUpdate();

            PreparedStatement add2 = db.prepareStatement("INSERT INTO friends VALUES (?, ?)");
            add2.setString(1, friendName);
            add2.setString(2, me);
            add2.executeUpdate();

            sendFriendList(me, conn);
            WebSocket friendConn = onlineUsers.get(friendName);
            if (friendConn != null) {
                sendFriendList(friendName, friendConn);
            }
        } catch (Exception e) {
            sendError(conn, "Ошибка добавления друга в БД: " + e.getMessage());
        }
    }

    private void handleMessage(WebSocket conn, JSONObject json) {
        String me = activeSessions.get(conn);
        if (me == null) return;

        String toUser = json.getString("to");
        String text = json.getString("text");

        globalHistory.add(new ChatMessage(me, toUser, text));

        WebSocket targetConn = onlineUsers.get(toUser);
        if (targetConn != null && targetConn.isOpen()) {
            JSONObject msgData = new JSONObject();
            msgData.put("from", me);
            msgData.put("text", text);
            sendResponse(targetConn, "MSG", msgData);
        }
    }

    private void handleSetAvatar(WebSocket conn, JSONObject json) {
        String me = activeSessions.get(conn);
        if (me == null) return;

        String base64Data = json.getString("avatar");

        try (Connection db = getConnection()) {
            PreparedStatement update = db.prepareStatement("UPDATE users SET avatar_base64 = ? WHERE username = ?");
            update.setString(1, base64Data);
            update.executeUpdate();

            System.out.println("Пользователь " + me + " сохранил новую аватарку в Postgres.");
            broadcastFriendListUpdate(me);
        } catch (Exception e) {
            sendError(conn, "Ошибка сохранения аватарки: " + e.getMessage());
        }
    }

    private void sendFriendList(String username, WebSocket conn) {
        if (conn == null || !conn.isOpen()) return;

        try (Connection db = getConnection()) {
            PreparedStatement query = db.prepareStatement(
                    "SELECT u.username, u.user_code, u.avatar_base64 FROM friends f " +
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
                fJson.put("avatar", rs.getString("avatar_base64"));
                array.put(fJson);
            }
            sendResponse(conn, "FRIENDS_LIST", new JSONObject().put("list", array));
        } catch (Exception e) {
            System.err.println("Ошибка выгрузки списка друзей: " + e.getMessage());
        }
    }

    private void sendChatHistory(String username, WebSocket conn) {
        JSONArray array = new JSONArray();
        for (ChatMessage cm : globalHistory) {
            if (cm.from.equals(username) || cm.to.equals(username)) {
                JSONObject m = new JSONObject();
                m.put("from", cm.from);
                m.put("to", cm.to);
                m.put("text", cm.message);
                array.put(m);
            }
        }
        sendResponse(conn, "HISTORY", new JSONObject().put("history", array));
    }

    private void broadcastFriendListUpdate(String username) {
        try (Connection db = getConnection()) {
            PreparedStatement query = db.prepareStatement("SELECT friend_name FROM friends WHERE username = ?");
            query.setString(1, username);
            ResultSet rs = query.executeQuery();

            while (rs.next()) {
                String friendName = rs.getString("friend_name");
                WebSocket friendConn = onlineUsers.get(friendName);
                if (friendConn != null) {
                    sendFriendList(friendName, friendConn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendResponse(WebSocket conn, String type, Object data) {
        JSONObject resp = new JSONObject();
        resp.put("type", type);
        resp.put("data", data);
        if (conn != null && conn.isOpen()) {
            conn.send(resp.toString());
        }
    }

    private void sendError(WebSocket conn, String errorMsg) {
        sendResponse(conn, "ERROR", errorMsg);
    }

    @Override public void onError(WebSocket conn, Exception ex) {
        System.err.println("Внутренняя ошибка сокета сервера: " + ex.getMessage());
    }

    @Override public void onStart() {
        System.out.println("WebSocket-сервер RenaBile успешно запущен на порту: " + getPort());
    }

    public static void main(String[] args) {
        int port = 8080;
        String portEnv = System.getenv("PORT");
        if (portEnv != null) {
            port = Integer.parseInt(portEnv);
        }
        ServerMain server = new ServerMain(port);
        server.start();
    }
}
