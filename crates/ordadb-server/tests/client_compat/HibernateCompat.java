import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.List;
import java.util.Properties;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

/** Bounded Hibernate ORM acceptance adapter. The PowerShell runner owns its hard timeout. */
public final class HibernateCompat {
    private HibernateCompat() {}

    public static void main(String[] args) {
        try {
            run();
            System.out.println("{\"schemaVersion\":1,\"adapter\":\"hibernate\",\"status\":\"completed\",\"results\":[{\"caseId\":\"hibernate-orm-001\",\"status\":\"passed\"}]}");
        } catch (Throwable error) {
            System.out.println("{\"schemaVersion\":1,\"adapter\":\"hibernate\",\"status\":\"failed\",\"results\":[{\"caseId\":\"hibernate-orm-001\",\"status\":\"failed\",\"diagnostic\":\"" + escape(error.getClass().getSimpleName()) + "\"}]}");
            System.exit(1);
        }
    }

    private static void run() {
        String host = requiredEnvironment("ORDADB_PG18_HOST");
        String port = requiredEnvironment("ORDADB_PG18_PORT");
        String database = requiredEnvironment("ORDADB_PG18_DATABASE");
        String user = requiredEnvironment("ORDADB_PG18_USER");
        String password = requiredEnvironment("ORDADB_PG18_PASSWORD");
        String sslMode = requiredEnvironment("ORDADB_PG18_SSLMODE");
        String rootCertificate = requiredEnvironment("ORDADB_PG18_ROOT_CERT");

        Properties settings = new Properties();
        settings.setProperty("hibernate.connection.url", "jdbc:postgresql://" + host + ":" + port + "/" + database);
        settings.setProperty("hibernate.connection.username", user);
        settings.setProperty("hibernate.connection.password", password);
        settings.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver");
        settings.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        settings.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        settings.setProperty("hibernate.show_sql", "false");
        settings.setProperty("hibernate.format_sql", "false");
        settings.setProperty("hibernate.highlight_sql", "false");
        settings.setProperty("hibernate.generate_statistics", "false");
        settings.setProperty("hibernate.connection.sslmode", sslMode);
        settings.setProperty("hibernate.connection.sslrootcert", rootCertificate);
        settings.setProperty("hibernate.connection.ApplicationName", "ordadb-pg18-hibernate-compat");

        Configuration configuration = new Configuration();
        configuration.setProperties(settings);
        configuration.addAnnotatedClass(CompatEntity.class);
        try (SessionFactory factory = configuration.buildSessionFactory()) {
            try (Session session = factory.openSession()) {
                Transaction transaction = session.beginTransaction();
                session.persist(new CompatEntity(1L, "hibernate-one"));
                transaction.commit();
            }
            try (Session session = factory.openSession()) {
                List<CompatEntity> rows = session.createQuery(
                        "from CompatEntity where id = :id", CompatEntity.class)
                        .setParameter("id", 1L)
                        .setMaxResults(2)
                        .getResultList();
                require(rows.size() == 1, "Hibernate query returned an unexpected row count");
                require("hibernate-one".equals(rows.get(0).payload), "Hibernate query returned an unexpected value");
            }
            try (Session session = factory.openSession()) {
                Transaction transaction = session.beginTransaction();
                session.persist(new CompatEntity(2L, "rolled-back"));
                transaction.rollback();
            }
            try (Session session = factory.openSession()) {
                Long count = session.createQuery(
                        "select count(e) from CompatEntity e where e.id = :id", Long.class)
                        .setParameter("id", 2L)
                        .getSingleResult();
                require(count == 0L, "Hibernate rollback published the rolled-back entity");
            }
        } finally {
            settings.remove("hibernate.connection.password");
            password = null;
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

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Entity(name = "CompatEntity")
    @Table(name = "ordadb_compat_hibernate_probe")
    public static final class CompatEntity {
        @Id
        private Long id;

        @Column(nullable = false, length = 128)
        private String payload;

        public CompatEntity() {}

        private CompatEntity(Long id, String payload) {
            this.id = id;
            this.payload = payload;
        }
    }
}
