package com.renalcraft.messenger.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseManager {

    private static Connection connection;

    // Твой точный External Database URL из панели управления Render
    private static final String RENDER_DB_URL = "postgresql://renabile_db_user:Z6A4Hq5tNq639FAyWbJFaQjeUFQVYa78@dpg-d8drpdmk1jcs739b1t60-a.frankfurt-postgres.render.com/renabile_db";

    public static Connection getConnection() {
        if (connection == null) {
            try {
                // 1. Принудительно регистрируем драйвер PostgreSQL в системе
                Class.forName("org.postgresql.Driver");

                // 2. Исправляем URL: Java требует, чтобы строка начиналась с "jdbc:"
                String jdbcUrl = RENDER_DB_URL;
                if (!jdbcUrl.startsWith("jdbc:")) {
                    jdbcUrl = "jdbc:" + jdbcUrl;
                }

                System.out.println("[SERVER-DB] Попытка подключения к базе данных PostgreSQL...");

                // 3. Открываем соединение с базой данных Render
                connection = DriverManager.getConnection(jdbcUrl);
                System.out.println("[SERVER-DB] Ура! Подключение к PostgreSQL успешно установлено!");

                // 4. Автоматически создаем таблицу пользователей, если база данных пустая
                initTables();

            } catch (ClassNotFoundException e) {
                System.err.println("[SERVER-DB] ОШИБКА: Драйвер PostgreSQL отсутствует! Проверь build.gradle.");
                e.printStackTrace();
            } catch (SQLException e) {
                System.err.println("[SERVER-DB] ОШИБКА: Не удалось установить соединение с БД Render.");
                e.printStackTrace();
            }
        }
        return connection;
    }

    private static void initTables() {
        // SQL-запрос для создания таблицы пользователей
        String createUsersTable = "CREATE TABLE IF NOT EXISTS users (" +
                "id SERIAL PRIMARY KEY, " +
                "username VARCHAR(50) UNIQUE NOT NULL, " +
                "password VARCHAR(100) NOT NULL, " +
                "code VARCHAR(10) UNIQUE NOT NULL" +
                ");";

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createUsersTable);
            System.out.println("[SERVER-DB] Проверка структуры БД: таблицы пользователей готовы к работе.");
        } catch (SQLException e) {
            System.err.println("[SERVER-DB] ОШИБКА: Не удалось создать таблицы в базе данных:");
            e.printStackTrace();
        }
    }
}
