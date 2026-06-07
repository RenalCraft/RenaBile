package com.renalcraft.messenger.server;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ServerMain extends WebSocketServer {

    private static final int DEFAULT_PORT = 8080;
    private static Connection dbConn = null;
    private static String databaseUrl = null;

    // Thread-safe map: userCode -> Active WebSocket connection
    private final Map<String, WebSocket> activeConnections = new ConcurrentHashMap<>();
    // Reverse map: WebSocket -> authenticated userCode (to manage offlines faster)
    private final Map<WebSocket, String> socketUserCodes = new ConcurrentHashMap<>();

    public ServerMain(int port) {
        super(new InetSocketAddress(port));
    }

    public static void main(String[] args) {
        // Resolve PORT
        int port = DEFAULT_PORT;
        String portEnv = System.getenv("PORT");
        if (portEnv != null && !portEnv.isEmpty()) {
            try {
                port = Integer.parseInt(portEnv);
            } catch (NumberFormatException e) {
                System.err.println("[SERVER] Custom port parse error, utilizing fallback 8080");
            }
        }

        // Initialize Database URL
        String rawDatabaseUrl = System.getenv("DATABASE_URL");
        if (rawDatabaseUrl == null || rawDatabaseUrl.isEmpty()) {
            System.out.println("[WARNING] DATABASE_URL env variable is not set! SQL connections cannot establish.");
        } else {
            rawDatabaseUrl = rawDatabaseUrl.trim();
            databaseUrl = convertToJdbcUrl(rawDatabaseUrl);
            System.out.println("[SERVER] Database URL converted to standard JDBC format.");
        }

        // Setup tables
        initDbSchema();

        // Start server
        ServerMain server = new ServerMain(port);
        server.start();
        System.out.println("[ServerMain] Server successfully started on port: " + port);
    }

    public static String convertToJdbcUrl(String rawUrl) {
        if (rawUrl == null || rawUrl.isEmpty()) {
            return rawUrl;
        }
        rawUrl = rawUrl.trim();
        if (rawUrl.startsWith("jdbc:postgresql://")) {
            return rawUrl;
        }
        if (!rawUrl.startsWith("postgresql://") && !rawUrl.startsWith("postgres://")) {
            return rawUrl;
        }

        try {
            // Strip scheme
            String withoutScheme = rawUrl.substring(rawUrl.indexOf("://") + 3);

            // Separate path and query from authority
            String authorityAndPath;
            String query = "";
            int dummyQuestionMark = withoutScheme.indexOf('?');
            if (dummyQuestionMark != -1) {
                authorityAndPath = withoutScheme.substring(0, dummyQuestionMark);
                query = withoutScheme.substring(dummyQuestionMark + 1);
            } else {
                authorityAndPath = withoutScheme;
            }

            int firstSlash = authorityAndPath.indexOf('/');
            String authority;
            String dbName = "";
            if (firstSlash != -1) {
                authority = authorityAndPath.substring(0, firstSlash);
                dbName = authorityAndPath.substring(firstSlash + 1);
            } else {
                authority = authorityAndPath;
            }

            // Parse user info and host
            String userInfo = "";
            String hostAndPort = authority;
            int atIndex = authority.lastIndexOf('@');
            if (atIndex != -1) {
                userInfo = authority.substring(0, atIndex);
                hostAndPort = authority.substring(atIndex + 1);
            }

            String username = "";
            String password = "";
            if (!userInfo.isEmpty()) {
                int colonIndex = userInfo.indexOf(':');
                if (colonIndex != -1) {
                    username = userInfo.substring(0, colonIndex);
                    password = userInfo.substring(colonIndex + 1);
                } else {
                    username = userInfo;
                }
            }

            // Parse host and port
            String host = hostAndPort;
            String port = "5432";
            int colonHostPort = hostAndPort.lastIndexOf(':');
            if (colonHostPort != -1 && colonHostPort > hostAndPort.lastIndexOf(']')) {
                host = hostAndPort.substring(0, colonHostPort);
                port = hostAndPort.substring(colonHostPort + 1);
            }

            // Construct JDBC URL: jdbc:postgresql://host:port/dbName
            StringBuilder jdbcUrl = new StringBuilder("jdbc:postgresql://");
            jdbcUrl.append(host).append(":").append(port).append("/").append(dbName);

            // Build parameters
            Map<String, String> params = new LinkedHashMap<>();
            if (!username.isEmpty()) {
                params.put("user", username);
            }
            if (!password.isEmpty()) {
                params.put("password", password);
            }

            // Default SSL for cloud/render
            if (rawUrl.contains("render.com") || rawUrl.contains("aws") || rawUrl.contains("neon.tech") || rawUrl.contains("elephantsql")) {
                params.put("sslmode", "require");
            }

            // If there's original query parameters, parse them too, avoiding duplicates
            if (!query.isEmpty()) {
                String[] queryParts = query.split("&");
                for (String part : queryParts) {
                    String[] kv = part.split("=", 2);
                    if (kv.length == 2) {
                        params.put(kv[0], kv[1]);
                    } else if (kv.length == 1) {
                        params.put(kv[0], "");
                    }
                }
            }

            // Append credentials and options
            if (!params.isEmpty()) {
                jdbcUrl.append("?");
                List<String> paramList = new ArrayList<>();
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    paramList.add(entry.getKey() + "=" + entry.getValue());
                }
                jdbcUrl.append(String.join("&", paramList));
            }

            return jdbcUrl.toString();
        } catch (Exception e) {
            System.err.println("[Database URL Parser] Manual parsing error: " + e.getMessage());
            // Safe fallback
            String fallback = rawUrl;
            if (fallback.startsWith("postgresql://")) {
                fallback = "jdbc:" + fallback;
            } else if (fallback.startsWith("postgres://")) {
                fallback = "jdbc:postgresql://" + fallback.substring(11);
            }
            return fallback;
        }
    }

    private static synchronized Connection getConnection() throws SQLException {
        if (dbConn == null || dbConn.isClosed()) {
            if (databaseUrl == null) {
                throw new SQLException("DATABASE_URL not configured. Ensure Environment Variables are updated on Render.");
            }
            dbConn = DriverManager.getConnection(databaseUrl);
        }
        return dbConn;
    }

    private static void initDbSchema() {
        if (databaseUrl == null) return;
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Verify Users Table
            stmt.execute("CREATE TABLE IF NOT EXISTS users (" +
                    "username VARCHAR(255) PRIMARY KEY, " +
                    "password VARCHAR(255) NOT NULL, " +
                    "code VARCHAR(4) UNIQUE NOT NULL, " +
                    "avatar TEXT, " +
                    "online BOOLEAN DEFAULT FALSE" +
                    ");");
            System.out.println("[DB] Users table verified.");

            // Migration alter scripts (safe even if columns already exist)
            try {
                stmt.execute("ALTER TABLE users DROP COLUMN IF EXISTS user_code CASCADE;");
            } catch (SQLException e) {
                System.out.println("[DB Migration] Users drop user_code info: " + e.getMessage());
            }
            try {
                stmt.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password VARCHAR(255);");
            } catch (SQLException e) {
                System.out.println("[DB Migration] Users alter password info: " + e.getMessage());
            }
            try {
                stmt.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS code VARCHAR(4);");
            } catch (SQLException e) {
                System.out.println("[DB Migration] Users alter code info: " + e.getMessage());
            }
            try {
                stmt.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar TEXT;");
            } catch (SQLException e) {
                System.out.println("[DB Migration] Users alter avatar info: " + e.getMessage());
            }
            try {
                stmt.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS online BOOLEAN DEFAULT FALSE;");
            } catch (SQLException e) {
                System.out.println("[DB Migration] Users alter online info: " + e.getMessage());
            }
            try {
                stmt.execute("ALTER TABLE users ADD CONSTRAINT unique_code UNIQUE (code);");
            } catch (SQLException ignored) {}

            // Verify Friendships Table
            stmt.execute("CREATE TABLE IF NOT EXISTS friendships (" +
                    "user_code VARCHAR(4) NOT NULL, " +
                    "friend_code VARCHAR(4) NOT NULL, " +
                    "PRIMARY KEY (user_code, friend_code)" +
                    ");");
            System.out.println("[DB] Friendships table verified.");

            // Verify Messages Table
            stmt.execute("CREATE TABLE IF NOT EXISTS messages (" +
                    "id SERIAL PRIMARY KEY, " +
                    "room VARCHAR(255) NOT NULL, " +
                    "sender VARCHAR(255) NOT NULL, " +
                    "sender_code VARCHAR(4) NOT NULL, " +
                    "text TEXT NOT NULL, " +
                    "time VARCHAR(30) NOT NULL, " +
                    "timestamp BIGINT NOT NULL" +
                    ");");
            System.out.println("[DB] Messages table verified.");

            // Migration alter lines for message tables
            try {
                stmt.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender_code VARCHAR(4);");
            } catch (SQLException e) {
                System.out.println("[DB Migration] Messages alter info: " + e.getMessage());
            }

            // Mark everyone offline on startup
            stmt.execute("UPDATE users SET online = false;");
            System.out.println("[DB] Marked all static user profiles offline.");

        } catch (SQLException e) {
            System.err.println("[DB] Critical failure setting up DB Tables: " + e.getMessage());
        }
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("[Connection] Client established connection. IP: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("[Connection] Client socket closed: " + conn.getRemoteSocketAddress());
        String userCode = socketUserCodes.remove(conn);
        if (userCode != null) {
            activeConnections.remove(userCode);
            try (Connection db = getConnection(); PreparedStatement ps = db.prepareStatement("UPDATE users SET online = false WHERE code = ?")) {
                ps.setString(1, userCode);
                ps.executeUpdate();
                System.out.println("[Status] User offline: #" + userCode);

                // Alert teammates of status change
                broadcastToFriends(userCode);
            } catch (SQLException e) {
                System.err.println("[Connection] Error setting offline in DB: " + e.getMessage());
            }
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // Handle incoming lines as client can package messages with \n
        String[] lines = message.split("\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;

            try {
                JSONObject packet = new JSONObject(trimmed);
                handlePacket(conn, packet);
            } catch (Exception e) {
                System.err.println("[Parser] Failed to process parsed packet line: " + e.getMessage());
                sendError(conn, "Неправильный формат запроса");
            }
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("[Socket Error] Exception thrown on socket: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("[Server Engine] Listening for incoming secure WebSocket handshake requests...");
    }

    private void handlePacket(WebSocket conn, JSONObject packet) throws Exception {
        String type = packet.optString("type");
        JSONObject data = packet.optJSONObject("data");

        if (type == null || type.isEmpty() || data == null) {
            sendError(conn, "Метаданные пакета повреждены");
            return;
        }

        System.out.println("[Server Packet] Received packet type: " + type);

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
            case "GET_HISTORY":
                handleGetHistory(conn, data);
                break;
            case "MSG":
                handleSendMessage(conn, data);
                break;
            default:
                sendError(conn, "Неизвестный тип пакета: " + type);
        }
    }

    private void handleRegister(WebSocket conn, JSONObject data) {
        String username = data.optString("username", "").trim();
        String password = data.optString("password", "").trim().toLowerCase();
        String avatar = data.optString("avatar", "");

        if (username.isEmpty() || password.isEmpty()) {
            sendError(conn, "Заполните все обязательные поля");
            return;
        }

        try (Connection db = getConnection()) {
            // Check username availability
            try (PreparedStatement psUser = db.prepareStatement("SELECT 1 FROM users WHERE LOWER(username) = ? LIMIT 1")) {
                psUser.setString(1, username.toLowerCase());
                try (ResultSet rs = psUser.executeQuery()) {
                    if (rs.next()) {
                        sendError(conn, "Код ошибки 409: Пользователь с таким именем уже существует");
                        return;
                    }
                }
            }

            // Create unique code
            String code = generateUniqueCode(db);

            // Register
            try (PreparedStatement psReg = db.prepareStatement("INSERT INTO users (username, password, code, avatar, online) VALUES (?, ?, ?, ?, ?)")) {
                psReg.setString(1, username);
                psReg.setString(2, password);
                psReg.setString(3, code);
                psReg.setString(4, avatar);
                psReg.setBoolean(5, true);
                psReg.executeUpdate();
            }

            // Link connection maps
            activeConnections.put(code, conn);
            socketUserCodes.put(conn, code);

            System.out.println("[REG] New Java User: " + username + " with code #" + code);

            // Send acceptance
            JSONObject authOk = new JSONObject();
            authOk.put("username", username);
            authOk.put("code", code);
            sendPacket(conn, "AUTH_OK", authOk);

            // Push empty lists since brand new account
            sendFriendsList(conn, code);

        } catch (SQLException e) {
            System.err.println("[REG] Critical SQL Error on setup: " + e.getMessage());
            sendError(conn, "Внутренняя ошибка сервера при генерации кода");
        }
    }

    private void handleAuth(WebSocket conn, JSONObject data) {
        String username = data.optString("username", "").trim();
        String password = data.optString("password", "").trim().toLowerCase();

        if (username.isEmpty() || password.isEmpty()) {
            sendError(conn, "Укажите имя пользователя и пароль");
            return;
        }

        try (Connection db = getConnection(); PreparedStatement ps = db.prepareStatement("SELECT username, password, code FROM users WHERE LOWER(username) = ? LIMIT 1")) {
            ps.setString(1, username.toLowerCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    sendError(conn, "Ошибка авторизации: Пользователь не существует");
                    return;
                }

                String actualUser = rs.getString("username");
                String actualPass = rs.getString("password").toLowerCase();
                String code = rs.getString("code");

                if (!actualPass.equals(password)) {
                    sendError(conn, "Ошибка авторизации: Неверный логин или пароль");
                    return;
                }

                // Update online status
                try (PreparedStatement psUpd = db.prepareStatement("UPDATE users SET online = true WHERE code = ?")) {
                    psUpd.setString(1, code);
                    psUpd.executeUpdate();
                }

                // Thread Maps binding
                activeConnections.put(code, conn);
                socketUserCodes.put(conn, code);

                System.out.println("[AUTH] Authenticated Java User: " + actualUser + " (#" + code + ")");

                // Response Approved
                JSONObject authOkObj = new JSONObject();
                authOkObj.put("username", actualUser);
                authOkObj.put("code", code);
                sendPacket(conn, "AUTH_OK", authOkObj);

                // Push Friends List
                sendFriendsList(conn, code);

                // Notify online friends of logging status
                broadcastToFriends(code);
            }
        } catch (SQLException e) {
            System.err.println("[AUTH] DB Query Connection Error: " + e.getMessage());
            sendError(conn, "Ошибка сервера при авторизации");
        }
    }

    private void handleAddFriend(WebSocket conn, JSONObject data) {
        String currentCode = socketUserCodes.get(conn);
        if (currentCode == null) {
            sendError(conn, "Сессия больше не валидна, требуется войти снова.");
            return;
        }

        String friendCode = data.optString("code", "").trim();
        if (friendCode.isEmpty()) {
            sendError(conn, "Введите корректный код потенциального собеседника.");
            return;
        }

        if (friendCode.equals(currentCode)) {
            sendError(conn, "Вы не можете добавить свой собственный код.");
            return;
        }

        try (Connection db = getConnection()) {
            // Find target username
            try (PreparedStatement psTarget = db.prepareStatement("SELECT 1 FROM users WHERE code = ? LIMIT 1")) {
                psTarget.setString(1, friendCode);
                try (ResultSet rs = psTarget.executeQuery()) {
                    if (!rs.next()) {
                        sendError(conn, "Пользователь со специальным кодом #" + friendCode + " не найден в базе.");
                        return;
                    }
                }
            }

            // Check if already friends
            try (PreparedStatement psCheck = db.prepareStatement("SELECT 1 FROM friendships WHERE user_code = ? AND friend_code = ? LIMIT 1")) {
                psCheck.setString(1, currentCode);
                psCheck.setString(2, friendCode);
                try (ResultSet rs = psCheck.executeQuery()) {
                    if (rs.next()) {
                        sendError(conn, "Пользователь уже находится в вашем списке чатов.");
                        return;
                    }
                }
            }

            // Insert mutual connections
            try (PreparedStatement psIns1 = db.prepareStatement("INSERT INTO friendships (user_code, friend_code) VALUES (?, ?)")) {
                psIns1.setString(1, currentCode);
                psIns1.setString(2, friendCode);
                psIns1.executeUpdate();
            }
            try (PreparedStatement psIns2 = db.prepareStatement("INSERT INTO friendships (user_code, friend_code) VALUES (?, ?)")) {
                psIns2.setString(1, friendCode);
                psIns2.setString(2, currentCode);
                psIns2.executeUpdate();
            }

            System.out.println("[Friendship] Mutually bound " + currentCode + " and " + friendCode);

            // Update caller list
            sendFriendsList(conn, currentCode);

            // Update friend list instantly if online
            WebSocket friendSocket = activeConnections.get(friendCode);
            if (friendSocket != null && friendSocket.isOpen()) {
                sendFriendsList(friendSocket, friendCode);
            }

        } catch (SQLException e) {
            System.err.println("[Friendship Exception] " + e.getMessage());
            sendError(conn, "Произошла ошибка при установлении контакта.");
        }
    }

    private void handleGetHistory(WebSocket conn, JSONObject data) {
        String currentCode = socketUserCodes.get(conn);
        if (currentCode == null) {
            sendError(conn, "Требуется авторизация");
            return;
        }

        String room = data.optString("room", "");
        if (room.isEmpty()) {
            sendError(conn, "Комната не указана");
            return;
        }

        System.out.println("[History] Java Socket Fetching log histories for room: " + room);

        try (Connection db = getConnection()) {
            JSONArray historyList = new JSONArray();

            if (room.equals("GLOBAL")) {
                try (PreparedStatement psHist = db.prepareStatement("SELECT sender, text, time FROM messages WHERE room = 'GLOBAL' ORDER BY timestamp ASC")) {
                    try (ResultSet rs = psHist.executeQuery()) {
                        while (rs.next()) {
                            JSONObject msgObj = new JSONObject();
                            msgObj.put("sender", rs.getString("sender"));
                            msgObj.put("text", rs.getString("text"));
                            msgObj.put("time", rs.getString("time"));
                            historyList.put(msgObj);
                        }
                    }
                }
            } else {
                try (PreparedStatement psHist = db.prepareStatement(
                        "SELECT sender, text, time FROM messages " +
                                "WHERE (room = ? AND sender_code = ?) " +
                                "   OR (room = ? AND sender_code = ?) " +
                                "ORDER BY timestamp ASC")) {
                    psHist.setString(1, room);
                    psHist.setString(2, currentCode);
                    psHist.setString(3, currentCode);
                    psHist.setString(4, room);

                    try (ResultSet rs = psHist.executeQuery()) {
                        while (rs.next()) {
                            JSONObject msgObj = new JSONObject();
                            msgObj.put("sender", rs.getString("sender"));
                            msgObj.put("text", rs.getString("text"));
                            msgObj.put("time", rs.getString("time"));
                            historyList.put(msgObj);
                        }
                    }
                }
            }

            JSONObject responseData = new JSONObject();
            responseData.put("room", room);
            responseData.put("history", historyList);
            sendPacket(conn, "MSG_HISTORY", responseData);

        } catch (SQLException e) {
            System.err.println("[History Exception] " + e.getMessage());
            sendError(conn, "Не удалось загрузить историю чата");
        }
    }

    private void handleSendMessage(WebSocket conn, JSONObject data) {
        String currentCode = socketUserCodes.get(conn);
        if (currentCode == null) {
            sendError(conn, "Ошибка отправки: Сессия не авторизована.");
            return;
        }

        String to = data.optString("to", "");
        String text = data.optString("text", "").trim();
        String clientMsgId = data.optString("clientMsgId", "");

        if (to.isEmpty() || text.isEmpty()) {
            return;
        }

        try (Connection db = getConnection()) {
            // Find sender username and compute format
            String senderUsername = "Пользователь";
            try (PreparedStatement psUser = db.prepareStatement("SELECT username FROM users WHERE code = ? LIMIT 1")) {
                psUser.setString(1, currentCode);
                try (ResultSet rs = psUser.executeQuery()) {
                    if (rs.next()) {
                        senderUsername = rs.getString("username");
                    }
                }
            }

            long timestamp = System.currentTimeMillis();
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm", new Locale("ru", "RU"));
            String timeStr = sdf.format(new java.util.Date(timestamp));

            // Write into database logs
            try (PreparedStatement psIns = db.prepareStatement(
                    "INSERT INTO messages (room, sender, sender_code, text, time, timestamp) VALUES (?, ?, ?, ?, ?, ?)")) {
                psIns.setString(1, to);
                psIns.setString(2, senderUsername);
                psIns.setString(3, currentCode);
                psIns.setString(4, text);
                psIns.setString(5, timeStr);
                psIns.setLong(6, timestamp);
                psIns.executeUpdate();
            }

            if (to.equals("GLOBAL")) {
                // Circular Broadcast to ALL
                JSONObject forwardObj = new JSONObject();
                forwardObj.put("from", "GLOBAL");
                forwardObj.put("senderName", senderUsername);
                forwardObj.put("text", text);
                forwardObj.put("clientMsgId", clientMsgId);

                for (WebSocket socketClient : getConnections()) {
                    if (socketClient.isOpen()) {
                        sendPacket(socketClient, "MSG", forwardObj);
                    }
                }
            } else {
                // Private message
                JSONObject forwardPrivateFriend = new JSONObject();
                forwardPrivateFriend.put("from", currentCode); // Sent from this sender code
                forwardPrivateFriend.put("senderName", senderUsername);
                forwardPrivateFriend.put("text", text);
                forwardPrivateFriend.put("clientMsgId", clientMsgId);

                WebSocket friendSocket = activeConnections.get(to);
                if (friendSocket != null && friendSocket.isOpen()) {
                    sendPacket(friendSocket, "MSG", forwardPrivateFriend);
                }

                // Send mirrored frame back to sender tab so UI appends it instantly
                JSONObject forwardPrivateSender = new JSONObject();
                forwardPrivateSender.put("from", to); // Under userCode of friend's active room
                forwardPrivateSender.put("senderName", senderUsername);
                forwardPrivateSender.put("text", text);
                forwardPrivateSender.put("clientMsgId", clientMsgId);
                sendPacket(conn, "MSG", forwardPrivateSender);
            }

        } catch (SQLException e) {
            System.err.println("[MSG Exception] " + e.getMessage());
        }
    }

    private void sendError(WebSocket conn, String info) {
        JSONObject obj = new JSONObject();
        obj.put("message", info);
        sendPacket(conn, "ERROR", obj);
    }

    private void sendPacket(WebSocket conn, String type, JSONObject data) {
        if (conn == null || !conn.isOpen()) return;
        JSONObject container = new JSONObject();
        container.put("type", type);
        container.put("data", data);
        conn.send(container.toString() + "\n");
    }

    private void sendFriendsList(WebSocket conn, String userCode) {
        try (Connection db = getConnection(); PreparedStatement ps = db.prepareStatement(
                "SELECT u.username, u.code, u.online, u.avatar " +
                        "FROM friendships f " +
                        "JOIN users u ON f.friend_code = u.code " +
                        "WHERE f.user_code = ?")) {
            ps.setString(1, userCode);
            try (ResultSet rs = ps.executeQuery()) {
                JSONArray friendsArray = new JSONArray();
                while (rs.next()) {
                    JSONObject friend = new JSONObject();
                    friend.put("username", rs.getString("username"));
                    friend.put("code", rs.getString("code"));
                    friend.put("online", rs.getBoolean("online"));
                    friend.put("avatar", rs.getString("avatar") != null ? rs.getString("avatar") : "");
                    friendsArray.put(friend);
                }

                JSONObject listPayload = new JSONObject();
                listPayload.put("list", friendsArray);
                sendPacket(conn, "FRIENDS_LIST", listPayload);
            }
        } catch (SQLException e) {
            System.err.println("[Friendship Retrieval Failure] " + e.getMessage());
        }
    }

    private void broadcastToFriends(String userCode) {
        try (Connection db = getConnection(); PreparedStatement ps = db.prepareStatement("SELECT friend_code FROM friendships WHERE user_code = ?")) {
            ps.setString(1, userCode);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String friendCode = rs.getString("friend_code");
                    WebSocket friendSocket = activeConnections.get(friendCode);
                    if (friendSocket != null && friendSocket.isOpen()) {
                        sendFriendsList(friendSocket, friendCode);
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("[Broadcast Failure] " + e.getMessage());
        }
    }

    private String generateUniqueCode(Connection db) throws SQLException {
        Random rand = new Random();
        for (int attempt = 0; attempt < 10000; attempt++) {
            int num = rand.nextInt(10000);
            String codeStr = String.format("%04d", num);

            try (PreparedStatement psCheck = db.prepareStatement("SELECT 1 FROM users WHERE code = ? LIMIT 1")) {
                psCheck.setString(1, codeStr);
                try (ResultSet rs = psCheck.executeQuery()) {
                    if (!rs.next()) {
                        return codeStr;
                    }
                }
            }
        }
        throw new SQLException("Unable to generate unique 4-digit code. Range exceeded.");
    }
}
