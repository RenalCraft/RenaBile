import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ServerMain extends WebSocketServer {

    static class User {
        String username;
        String password;
        String userCode;
        WebSocket connection;
        List<String> friends = new CopyOnWriteArrayList<>();

        User(String username, String password, String userCode) {
            this.username = username;
            this.password = password;
            this.userCode = userCode;
        }
    }

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

    private final Map<String, User> usersByNames = new ConcurrentHashMap<>();
    private final Map<String, User> usersByCodes = new ConcurrentHashMap<>();
    private final Map<WebSocket, User> activeSessions = new ConcurrentHashMap<>();
    private final List<ChatMessage> globalHistory = new CopyOnWriteArrayList<>();

    public ServerMain(int port) {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        System.out.println("Новое сырое подключение: " + conn.getRemoteSocketAddress());
        System.out.flush();
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        User user = activeSessions.remove(conn);
        if (user != null) {
            user.connection = null;
            System.out.println("Пользователь " + user.username + " ушел в оффлайн.");
            broadcastFriendListUpdate(user);
        }
        System.out.flush();
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        try {
            JSONObject json = new JSONObject(message);
            String type = json.getString("type");

            switch (type) {
                case "REG":
                    handleRegister(conn, json);
                    break;
                case "AUTH":
                    handleAuth(conn, json);
                    break;
                case "ADD_FRIEND":
                    handleWithFriend(conn, json);
                    break;
                case "MSG":
                    handleMessage(conn, json);
                    break;
            }
        } catch (Exception e) {
            sendError(conn, "Неверный формат запроса: " + e.getMessage());
        }
        System.out.flush();
    }

    private void handleRegister(WebSocket conn, JSONObject json) {
        String user = json.getString("username").trim();
        String pass = json.getString("password").trim();

        if (usersByNames.containsKey(user)) {
            sendResponse(conn, "REG_FAIL", "Имя пользователя уже занято!");
            return;
        }

        String code;
        Random rand = new Random();
        do {
            code = "#" + (1000 + rand.nextInt(9000));
        } while (usersByCodes.containsKey(code));

        User newUser = new User(user, pass, code);
        usersByNames.put(user, newUser);
        usersByCodes.put(code, newUser);

        System.out.println("Зарегистрирован игрок: " + user + " с кодом " + code);
        sendResponse(conn, "REG_OK", new JSONObject().put("code", code));
    }

    private void handleAuth(WebSocket conn, JSONObject json) {
        String user = json.getString("username").trim();
        String pass = json.getString("password").trim();

        User existingUser = usersByNames.get(user);
        if (existingUser == null || !existingUser.password.equals(pass)) {
            sendResponse(conn, "AUTH_FAIL", "Неверное имя или пароль!");
            return;
        }

        existingUser.connection = conn;
        activeSessions.put(conn, existingUser);

        System.out.println("Пользователь авторизован: " + user);

        JSONObject data = new JSONObject();
        data.put("code", existingUser.userCode);
        sendResponse(conn, "AUTH_OK", data);

        sendFriendList(existingUser);
        sendChatHistory(existingUser);
        broadcastFriendListUpdate(existingUser);
    }

    private void handleWithFriend(WebSocket conn, JSONObject json) {
        User me = activeSessions.get(conn);
        if (me == null) return;

        String targetCode = json.getString("code").trim();
        User friend = usersByCodes.get(targetCode);

        if (friend == null) {
            sendResponse(conn, "ADD_FRIEND_FAIL", "Пользователь с таким кодом не найден!");
            return;
        }
        if (friend.userCode.equals(me.userCode)) {
            sendResponse(conn, "ADD_FRIEND_FAIL", "Нельзя добавить самого себя!");
            return;
        }
        if (me.friends.contains(friend.username)) {
            sendResponse(conn, "ADD_FRIEND_FAIL", "Этот пользователь уже в друзьях!");
            return;
        }

        me.friends.add(friend.username);
        friend.friends.add(me.username);

        sendFriendList(me);
        if (friend.connection != null) {
            sendFriendList(friend);
        }
    }

    private void handleMessage(WebSocket conn, JSONObject json) {
        User me = activeSessions.get(conn);
        if (me == null) return;

        String toUser = json.getString("to");
        String text = json.getString("text");

        globalHistory.add(new ChatMessage(me.username, toUser, text));

        User target = usersByNames.get(toUser);
        if (target != null && target.connection != null && target.connection.isOpen()) {
            JSONObject msgData = new JSONObject();
            msgData.put("from", me.username);
            msgData.put("text", text);
            sendResponse(target.connection, "MSG", msgData);
        }
    }

    private void sendFriendList(User user) {
        if (user.connection == null) return;
        JSONArray array = new JSONArray();
        for (String fName : user.friends) {
            User fObj = usersByNames.get(fName);
            JSONObject fJson = new JSONObject();
            fJson.put("username", fName);
            fJson.put("code", fObj.userCode);
            fJson.put("online", fObj.connection != null);
            array.put(fJson);
        }
        sendResponse(user.connection, "FRIENDS_LIST", new JSONObject().put("list", array));
    }

    private void sendChatHistory(User user) {
        if (user.connection == null) return;
        JSONArray array = new JSONArray();
        for (ChatMessage cm : globalHistory) {
            if ((cm.from.equals(user.username)) || (cm.to.equals(user.username))) {
                JSONObject m = new JSONObject();
                m.put("from", cm.from);
                m.put("to", cm.to);
                m.put("text", cm.text);
                array.put(m);
            }
        }
        sendResponse(user.connection, "HISTORY", new JSONObject().put("history", array));
    }

    private void broadcastFriendListUpdate(User user) {
        for (String fName : user.friends) {
            User friend = usersByNames.get(fName);
            if (friend != null) {
                sendFriendList(friend);
            }
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

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("Ошибка сервера: " + ex.getMessage());
        if (conn != null) activeSessions.remove(conn);
    }

    @Override
    public void onStart() {
        System.out.println("Обновленный Сервер RenaBile запущен на порту: " + getPort());
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
