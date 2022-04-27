package test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import src.OlympicDBAccess;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

class OlympicDBAccessTest {
    private static OlympicDBAccess db;
    private static final Logger logger =
            Logger.getLogger(OlympicDBAccessTest.class.getName());

    @BeforeAll
    static void connect() {
        db = new OlympicDBAccess();
    }

    @Test
    void createTables() {
        db.createTables();
        try {
            ResultSet rs = db.executeSQL("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'z5414201' LIMIT 4;");
            rs.absolute(1);
            assertEquals(4, rs.getInt(1));
        } catch (SQLException e) {
            logger.warning("Unable to execute sql: " + e.getMessage());
            fail();
        }
    }

    @Test
    void dropTables() {
        db.createTables();
        db.dropTables();
        try {
            ResultSet rs = db.executeSQL("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'z5414201' LIMIT 4;");
            rs.absolute(1);
            assertEquals(0, rs.getInt(1));
        } catch (SQLException e) {
            logger.warning("Unable to execute sql: " + e.getMessage());
            fail();
        }
    }

    @Test
    void populateTables() {
        fail();
    }

    @Test
    void runQueries() {
        fail();
    }
}