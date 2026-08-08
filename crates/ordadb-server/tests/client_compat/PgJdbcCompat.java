import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

/** Bounded pgJDBC acceptance adapter. The PowerShell runner owns its hard timeout. */
public final class PgJdbcCompat {
    private static final String TABLE = "public.ordadb_compat_client_probe";
    private static final String COPY_TABLE = "public.ordadb_compat_copy_probe";
    private static final List<Result> RESULTS = new ArrayList<>();

    private PgJdbcCompat() {}

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            run();
        } catch (Throwable error) {
            exitCode = 1;
            RESULTS.add(new Result("pgjdbc-suite", "failed", safeDiagnostic(error)));
        }
        System.out.println(toJson(exitCode == 0 ? "completed" : "failed"));
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static void run() throws Exception {
        String host = requiredEnvironment("ORDADB_PG18_HOST");
        String port = requiredEnvironment("ORDADB_PG18_PORT");
        String database = requiredEnvironment("ORDADB_PG18_DATABASE");
        String user = requiredEnvironment("ORDADB_PG18_USER");
        String password = requiredEnvironment("ORDADB_PG18_PASSWORD");
        String sslMode = requiredEnvironment("ORDADB_PG18_SSLMODE");
        String rootCertificate = requiredEnvironment("ORDADB_PG18_ROOT_CERT");

        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("sslmode", sslMode);
        properties.setProperty("sslrootcert", rootCertificate);
        properties.setProperty("ApplicationName", "ordadb-pg18-pgjdbc-compat");
        properties.setProperty("connectTimeout", "10");
        properties.setProperty("socketTimeout", "30");
        properties.setProperty("options", "-c statement_timeout=30000");

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            verifySession(connection);
            verifyCatalog(connection);
            cleanup(connection);
            try {
                verifyDdlCrud(connection);
                verifyPreparedPortal(connection);
                verifyTransactionSavepoint(connection);
                verifyCopy(connection);
                verifyCancellation(connection);
                verifyErrorRecovery(connection);
            } finally {
                cleanup(connection);
            }
        } finally {
            password = null;
            properties.remove("password");
        }
    }

    private static void verifySession(Connection connection) throws SQLException {
        require(connection.getMetaData().getDatabaseMajorVersion() == 18, "server did not advertise major version 18");
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "SELECT version(), current_database(), current_user, session_user")) {
            require(rows.next(), "session query returned no row");
            for (int column = 1; column <= 4; column++) {
                require(rows.getString(column) != null, "session query returned a null identity field");
            }
            require(!rows.next(), "session query exceeded its one-row bound");
        }
        RESULTS.add(new Result("session-startup-001", "passed", null));
    }

    private static void verifyCatalog(Connection connection) throws SQLException {
        String sql = "SELECT n.oid, n.nspname, r.rolname "
                + "FROM pg_catalog.pg_namespace n "
                + "LEFT JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner "
                + "WHERE n.nspname IN ('pg_catalog', 'information_schema', 'public') "
                + "ORDER BY n.nspname LIMIT 256";
        int rowsSeen = 0;
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
            while (rows.next()) {
                rowsSeen++;
                require(rowsSeen <= 256, "catalog query exceeded its row bound");
                require(rows.getString(2) != null, "catalog query returned a null schema name");
            }
        }
        require(rowsSeen > 0, "catalog query returned no visible schemas");
        RESULTS.add(new Result("catalog-schemas-001", "passed", null));
    }

    private static void verifyDdlCrud(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)");
        }
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + TABLE + " (id, payload) VALUES (?, ?)")) {
            for (long id = 1; id <= 3; id++) {
                insert.setLong(1, id);
                insert.setString(2, "value-" + id);
                require(insert.executeUpdate() == 1, "prepared insert affected an unexpected row count");
            }
        }
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE " + TABLE + " SET payload = ? WHERE id = ?")) {
            update.setString(1, "updated");
            update.setLong(2, 2);
            require(update.executeUpdate() == 1, "prepared update affected an unexpected row count");
        }
        try (PreparedStatement delete = connection.prepareStatement(
                "DELETE FROM " + TABLE + " WHERE id = ?")) {
            delete.setLong(1, 3);
            require(delete.executeUpdate() == 1, "prepared delete affected an unexpected row count");
        }
        RESULTS.add(new Result("ddl-crud-001", "passed", null));
    }

    private static void verifyPreparedPortal(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        int rowsSeen = 0;
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT id, payload FROM " + TABLE + " WHERE id >= ? ORDER BY id LIMIT 32")) {
            query.setFetchSize(2);
            query.setLong(1, 1);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    rowsSeen++;
                    require(rowsSeen <= 32, "portal query exceeded its row bound");
                }
            }
            connection.commit();
        } finally {
            connection.setAutoCommit(true);
        }
        require(rowsSeen == 2, "portal query returned an unexpected row count");
        RESULTS.add(new Result("prepared-portal-001", "passed", null));
    }

    private static void verifyTransactionSavepoint(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        try {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO " + TABLE + " (id, payload) VALUES (4, 'kept')");
                Savepoint savepoint = connection.setSavepoint("ordadb_compat_sp");
                statement.executeUpdate("INSERT INTO " + TABLE + " (id, payload) VALUES (5, 'rolled-back')");
                connection.rollback(savepoint);
                connection.commit();
            }
        } finally {
            connection.setAutoCommit(true);
        }
        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT count(*) FROM " + TABLE + " WHERE id = 5")) {
            require(rows.next() && rows.getLong(1) == 0, "savepoint rollback published the rolled-back row");
        }
        RESULTS.add(new Result("transactions-savepoint-001", "passed", null));
    }

    private static void verifyCopy(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE " + COPY_TABLE + " (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)");
        }
        CopyManager copy = connection.unwrap(PGConnection.class).getCopyAPI();
        byte[] input = "1\tcopy-one\n2\tcopy-two\n".getBytes(StandardCharsets.UTF_8);
        long copiedIn = copy.copyIn(
                "COPY " + COPY_TABLE + " (id, payload) FROM STDIN WITH (FORMAT text)",
                new ByteArrayInputStream(input));
        require(copiedIn == 2, "COPY FROM STDIN reported an unexpected row count");
        RESULTS.add(new Result("copy-in-text-001", "passed", null));

        ByteArrayOutputStream output = new ByteArrayOutputStream(1024);
        long copiedOut = copy.copyOut(
                "COPY " + COPY_TABLE + " (id, payload) TO STDOUT WITH (FORMAT csv, HEADER true)",
                output);
        require(copiedOut == 2, "COPY TO STDOUT reported an unexpected row count");
        require(output.size() <= 1024, "COPY TO STDOUT exceeded its payload bound");
        String csv = output.toString(StandardCharsets.UTF_8.name()).replace("\r\n", "\n");
        require("id,payload\n1,copy-one\n2,copy-two\n".equals(csv), "COPY TO STDOUT returned unexpected CSV");
        RESULTS.add(new Result("copy-out-csv-001", "passed", null));
    }

    private static void verifyCancellation(Connection connection) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Statement> active = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Future<String> query = executor.submit(() -> {
            try (Statement statement = connection.createStatement()) {
                active.set(statement);
                started.countDown();
                statement.executeQuery("WITH RECURSIVE ordadb_cancel_probe(n) AS ("
                        + "SELECT 1 UNION ALL SELECT n + 1 FROM ordadb_cancel_probe WHERE n < 1000000"
                        + ") SELECT sum(n) FROM ordadb_cancel_probe");
                return "completed";
            } catch (SQLException error) {
                return error.getSQLState();
            }
        });
        try {
            require(started.await(5, TimeUnit.SECONDS), "cancellation statement did not start");
            Thread.sleep(50);
            Statement statement = active.get();
            require(statement != null, "cancellation statement was unavailable");
            statement.cancel();
            String outcome = query.get(20, TimeUnit.SECONDS);
            require("57014".equals(outcome), "cancellation did not return SQLSTATE 57014");
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery("SELECT 1")) {
            require(rows.next() && rows.getInt(1) == 1, "connection was not reusable after cancellation");
        }
        RESULTS.add(new Result("cancellation-001", "passed", null));
    }

    private static void verifyErrorRecovery(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try {
                statement.executeQuery("SELECT * FROM public.ordadb_compat_missing_relation");
                throw new IllegalStateException("undefined relation unexpectedly succeeded");
            } catch (SQLException error) {
                require("42P01".equals(error.getSQLState()), "undefined relation returned an unexpected SQLSTATE");
            }
            try (ResultSet rows = statement.executeQuery("SELECT 1")) {
                require(rows.next() && rows.getInt(1) == 1, "connection was not reusable after a statement error");
            }
        }
        RESULTS.add(new Result("error-simple-recovery-001", "passed", null));
    }

    private static void cleanup(Connection connection) throws SQLException {
        connection.setAutoCommit(true);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + COPY_TABLE);
            statement.executeUpdate("DROP TABLE IF EXISTS " + TABLE);
        }
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("required environment input is missing");
        }
        return value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static String safeDiagnostic(Throwable error) {
        Throwable current = error;
        if (error instanceof ExecutionException && error.getCause() != null) {
            current = error.getCause();
        }
        if (current instanceof SQLException) {
            String sqlState = ((SQLException) current).getSQLState();
            return "SQLException sqlstate=" + (sqlState == null ? "unknown" : sqlState);
        }
        return current.getClass().getSimpleName();
    }

    private static String toJson(String status) {
        StringBuilder json = new StringBuilder(256);
        json.append("{\"schemaVersion\":1,\"adapter\":\"pgjdbc\",\"status\":\"")
                .append(status)
                .append("\",\"results\":[");
        for (int index = 0; index < RESULTS.size(); index++) {
            if (index > 0) {
                json.append(',');
            }
            Result result = RESULTS.get(index);
            json.append("{\"caseId\":\"").append(escape(result.caseId))
                    .append("\",\"status\":\"").append(escape(result.status)).append('"');
            if (result.diagnostic != null) {
                json.append(",\"diagnostic\":\"").append(escape(result.diagnostic)).append('"');
            }
            json.append('}');
        }
        return json.append("]}").toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static final class Result {
        private final String caseId;
        private final String status;
        private final String diagnostic;

        private Result(String caseId, String status, String diagnostic) {
            this.caseId = caseId;
            this.status = status;
            this.diagnostic = diagnostic;
        }
    }
}
