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

    private static final String DEFAULT_HOST = "dpg-d8drpdmk1jcs739b1t60-a";
    private static final String DB_NAME = "renabile_db";
    private static final String DB_USER = "renabile_db_user";
    private static final String DB_PASS = "Z6A4Hq5tNq639FAyWbJFaQjeUFQVYa78";

    // --- СИСТЕМА ОБНОВЛЕНИЙ ---
    private static final String GITHUB_VERSION_URL = "https://raw.githubusercontent.com/RenalCraft/RenaBile-Updates/main/version.json";
    private static int latestVersionCode = 1;
    private static String latestVersionName = "v1.0";
    private static String latestApkUrl = "";
    // --------------------------

    public ServerMain(int port) {
        super(new InetSocketAddress(port));
    }

    private Connection getFreshConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("[БАЗА ДАННЫХ] Драйвер PostgreSQL не найден!");
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
                System.err.println("[БАЗА ДАННЫХ] Ошибка чтения конфигурации DATABASE_URL: " + e.getMessage());
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
        System.out.println("[СЕТЬ] Новое подключение: " + conn.getRemoteSocketAddress().getAddress().getHostAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        String username = activeSessions.remove(conn);
        if (username != null) {
            onlineUsers.remove(username);
            System.out.println("[СЕТЬ] Пользователь " + username + " вышел из сети.");
            broadcastFriendListUpdate(username);
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        try {
            JSONObject json = new JSONObject(message.trim());
            String rawType = json.optString("type", "").toUpperCase();

            if ("PING".equals(rawType)) {
                JSONObject pong = new JSONObject().put("type", "PONG");
                conn.send(pong.toString());
                return;
            }

            String type = rawType;
            if ("LOGIN".equals(rawType)) type = "AUTH";
            if ("REGISTER".equals(rawType)) type = "REG";
            if ("MESSAGE".equals(rawType)) type = "MSG";

            JSONObject data = json.has("data") ? json.getJSONObject("data") : json;

            switch (type) {
                case "REG":
                    handleRegister(conn, data);
                    break;
                case "AUTH":
                    handleAuth(conn, data);
                    break;
                case "UPDATE_PROFILE":
                    handleUpdateProfile(conn, data);
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
            }
        } catch (Exception e) {
            System.err.println("[СЕРВЕР] Ошибка обработки пакета: " + e.getMessage());
        }
    }

    private void handleRegister(WebSocket conn, JSONObject data) {
        String user = data.getString("username").trim();
        String pass = data.getString("password").trim();
        String avatar = data.optString("avatar", "").trim();
        int clientVersionCode = data.optInt("versionCode", 1); // Получаем версию от телефона

        try (Connection db = getFreshConnection()) {
            PreparedStatement check = db.prepareStatement("SELECT 1 FROM users WHERE username = ?");
            check.setString(1, user);
            if (check.executeQuery().next()) {
                sendResponse(conn, "ERROR", new JSONObject().put("message", "Этот логин уже занят!"));
                return;
            }

            String code; Random rand = new Random();
            PreparedStatement checkCode = db.prepareStatement("SELECT 1 FROM users WHERE user_code = ?");
            do {
                code = String.format("%04d", rand.nextInt(10000));
                checkCode.setString(1, code);
            } while (checkCode.executeQuery().next());

            PreparedStatement insert = db.prepareStatement("INSERT INTO users (username, password, user_code, avatar_base64) VALUES (?, ?, ?, ?)");
            insert.setString(1, user); insert.setString(2, pass); insert.setString(3, code); insert.setString(4, avatar);
            insert.executeUpdate();

            activeSessions.put(conn, user);
            onlineUsers.put(user, conn);

            JSONObject respData = new JSONObject()
                    .put("code", code)
                    .put("username", user)
                    .put("avatar_base64", avatar);
            sendResponse(conn, "AUTH_OK", respData);
            sendFriendList(user, conn);

            // Проверяем, нужно ли приложению обновиться
            checkAndSendUpdateNotification(conn, clientVersionCode);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleAuth(WebSocket conn, JSONObject data) {
        String user = data.getString("username").trim();
        String pass = data.getString("password").trim();
        int clientVersionCode = data.optInt("versionCode", 1); // Получаем версию от телефона

        try (Connection db = getFreshConnection()) {
            PreparedStatement query = db.prepareStatement("SELECT user_code, avatar_base64 FROM users WHERE username = ? AND password = ?");
            query.setString(1, user); query.setString(2, pass);
            ResultSet rs = query.executeQuery();

            if (!rs.next()) {
                sendResponse(conn, "ERROR", new JSONObject().put("message", "Неверный логин или пароль!"));
                return;
            }

            String code = rs.getString("user_code");
            String avatar = rs.getString("avatar_base64");
            activeSessions.put(conn, user);
            onlineUsers.put(user, conn);

            JSONObject respData = new JSONObject()
                    .put("code", code)
                    .put("username", user)
                    .put("avatar_base64", avatar != null ? avatar : "");
            sendResponse(conn, "AUTH_OK", respData);

            sendFriendList(user, conn);
            broadcastFriendListUpdate(user);

            // Проверяем, нужно ли приложению обновиться
            checkAndSendUpdateNotification(conn, clientVersionCode);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleUpdateProfile(WebSocket conn, JSONObject data) {
        String me = activeSessions.get(conn);
        if (me == null) return;

        String newPass = data.optString("password", "").trim();
        String newAvatar = data.optString("avatar", "").trim();

        try (Connection db = getFreshConnection()) {
            if (!newPass.isEmpty()) {
                PreparedStatement ps = db.prepareStatement("UPDATE users SET password = ? WHERE username = ?");
                ps.setString(1, newPass); ps.setString(2, me);
                ps.executeUpdate();
            }
            if (!newAvatar.isEmpty()) {
                PreparedStatement ps = db.prepareStatement("UPDATE users SET avatar_base64 = ? WHERE username = ?");
                ps.setString(1, newAvatar); ps.setString(2, me);
                ps.executeUpdate();
            }

            sendFriendList(me, conn);
            broadcastFriendListUpdate(me);
        } catch (Exception e) {
            System.err.println("[ОШИБКА БД] Обновление профиля: " + e.getMessage());
        }
    }

    private void handleAddFriend(WebSocket conn, JSONObject data) {
        String me = activeSessions.get(conn);
        if (me == null) return;
        String target = data.getString("code").trim();

        try (Connection db = getFreshConnection()) {
            PreparedStatement find = db.prepareStatement("SELECT username FROM users WHERE user_code = ?");
            find.setString(1, target);
            ResultSet rs = find.executeQuery();

            if (!rs.next()) return;
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

        String toTarget = data.optString("to", "GLOBAL");
        String text = data.optString("text", "");
        String senderCode = data.optString("fromCode", "GLOBAL");

        JSONObject msgData = new JSONObject()
                .put("from", toTarget.equals("GLOBAL") ? "GLOBAL" : senderCode)
                .put("senderName", me)
                .put("text", text);

        if (toTarget.equals("GLOBAL")) {
            for (WebSocket session : activeSessions.keySet()) {
                if (session.isOpen()) sendResponse(session, "MSG", msgData);
            }
        } else {
            sendResponse(conn, "MSG", msgData);
            try (Connection db = getFreshConnection()) {
                PreparedStatement find = db.prepareStatement("SELECT username FROM users WHERE user_code = ?");
                find.setString(1, toTarget);
                ResultSet rs = find.executeQuery();
                if (rs.next()) {
                    WebSocket targetConn = onlineUsers.get(rs.getString("username"));
                    if (targetConn != null && targetConn.isOpen() && targetConn != conn) {
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

                String dbAvatar = rs.getString("avatar_base64");
                fJson.put("avatar", dbAvatar != null ? dbAvatar : "");

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

    private void sendResponse(WebSocket conn, String type, JSONObject data) {
        JSONObject resp = new JSONObject();
        resp.put("type", type);
        resp.put("data", data);
        if (conn != null && conn.isOpen()) conn.send(resp.toString());
    }

    // --- МЕТОДЫ ДЛЯ АВТООБНОВЛЕНИЯ ЧЕРЕЗ СЕРВЕР ---

    private static void checkGitHubUpdates() {
        new Thread(() -> {
            try {
                java.net.URL url = new java.net.URL(GITHUB_VERSION_URL);
                java.net.HttpURLConnection conn = (java.net.HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(5000);

                java.io.InputStream in = new java.io.BufferedInputStream(conn.getInputStream());
                java.io.ByteArrayOutputStream bo = new java.io.ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    bo.write(buffer, 0, len);
                }

                JSONObject json = new JSONObject(bo.toString());
                latestVersionCode = json.optInt("versionCode", 1);
                latestVersionName = json.optString("versionName", "v1.0");
                latestApkUrl = json.optString("apkUrl", "");
                System.out.println("[ОБНОВЛЕНИЯ] Найдена актуальная версия на GitHub: " + latestVersionName + " (Code: " + latestVersionCode + ")");
            } catch (Exception e) {
                System.err.println("[ОБНОВЛЕНИЯ] Ошибка проверки файла конфигурации на GitHub: " + e.getMessage());
            }
        }).start();
    }

    private void checkAndSendUpdateNotification(WebSocket conn, int clientVersionCode) {
        if (latestVersionCode > clientVersionCode) {
            JSONObject updateData = new JSONObject()
                    .put("versionName", latestVersionName)
                    .put("apkUrl", latestApkUrl);
            sendResponse(conn, "NEW_UPDATE_AVAILABLE", updateData);
            System.out.println("[ОБНОВЛЕНИЯ] Клиенту отправлено уведомление о новой версии " + latestVersionName);
        }
    }

    @Override public void onError(WebSocket conn, Exception ex) {}
    @Override public void onStart() {
        System.out.println("[МЕНЕДЖЕР] Главный сервер запущен на порту 8080!");
        checkGitHubUpdates(); // Проверяем актуальный JSON при старте сервера
    }

    public static void main(String[] args) {
        int port = 8080;
        String portEnv = System.getenv("PORT");
        if (portEnv != null) port = Integer.parseInt(portEnv);
        new ServerMain(port).start();
    }
}
