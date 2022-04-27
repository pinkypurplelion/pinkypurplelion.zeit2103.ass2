package src;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.BiConsumer;
import java.util.logging.Logger;


public class OlympicDBAccess {
    Connection conn;

    private static final Logger logger =
            Logger.getLogger(OlympicDBAccess.class.getName());

    /**
     * Connects to the mySQL database.
     */
    public OlympicDBAccess() {
        String host = "seitux2.adfa.unsw.edu.au";
        String username = "z5414201";

        String databaseHost = "localhost";
        int databasePort = 3306;
        String databaseUsername = "z5414201";
        String databasePassword = "mysqlpass";

        JSch jsch = new JSch();

        try {
            // Connect to SSH to server containing mySQL server
            Session session = jsch.getSession(username, host);
            session.setPassword("2Wp5^cfgrE25agtE");

            Properties prop = new Properties();
            prop.put("StrictHostKeyChecking", "no"); // doesn't check SSH cert.
            session.setConfig(prop);
            session.connect();

            // Forward randomly chosen local port through the SSH channel to database host/port
            int forwardedPort = session.setPortForwardingL(0, databaseHost, databasePort);

            // Connect to the forwarded port (the local end of the SSH tunnel)
            String url = "jdbc:mysql://localhost:" + forwardedPort
                    + "/z5414201?useServerPrepStmts=false&rewriteBatchedStatements=true";
            conn = DriverManager.getConnection(url, databaseUsername, databasePassword);
            conn.setAutoCommit(false);
            logger.info("DB Connection Established");
        } catch (JSchException e) {
            logger.severe("Error connecting to the server: " + e.getMessage());
            throw new RuntimeException(e);
        } catch (SQLException e) {
            logger.severe("Error connecting to the sql database: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates all tables in the DB.
     */
    public void createTables() {
        String CREATE_OLYMPICS = "CREATE TABLE Olympics(" +
                "ID INT NOT NULL AUTO_INCREMENT," +
                "year INT," +
                "season VARCHAR(7)," +
                "city VARCHAR(23)," +
                "PRIMARY KEY (ID)," +
                "INDEX (year, season, city));";

        String CREATE_EVENTS = "CREATE TABLE Events(" +
                "ID INT NOT NULL AUTO_INCREMENT," +
                "sport VARCHAR(26)," +
                "event VARCHAR(86)," +
                "PRIMARY KEY (ID)," +
                "INDEX (sport, event));";

        String CREATE_ATHLETES = "CREATE TABLE Athletes(" +
                "ID INT NOT NULL AUTO_INCREMENT," +
                "name VARCHAR(94)," +
                "noc CHAR(3)," +
                "gender CHAR(1)," +
                "PRIMARY KEY (ID)," +
                "INDEX (name, noc, gender));";

        String CREATE_MEDALS = "CREATE TABLE Medals(" +
                "ID INT NOT NULL AUTO_INCREMENT," +
                "olympicID INT, FOREIGN KEY (olympicID) REFERENCES Olympics(ID)," +
                "eventID INT, FOREIGN KEY (eventID) REFERENCES Events(ID)," +
                "athleteID INT, FOREIGN KEY (athleteID) REFERENCES Athletes(ID)," +
                "medalColour VARCHAR(7)," +
                "PRIMARY KEY (ID));";

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs;
            String[] tables = new String[]{"Olympics", "Events", "Athletes", "Medals"};
            String[] sql = new String[]{CREATE_OLYMPICS, CREATE_EVENTS, CREATE_ATHLETES, CREATE_MEDALS};
            for (int i = 0; i < tables.length; i++) {
                rs = stmt.executeQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'z5414201' AND table_name = '" + tables[i] + "' LIMIT 1;");
                rs.absolute(1);
                if (rs.getInt(1) != 1) stmt.executeUpdate(sql[i]);
            }
            logger.info("All tables created!");
        } catch (SQLException e) {
            logger.warning("Unable to create all tables. Error: " + e.getMessage());
            throw new RuntimeException("Unable to create all tables. " + e);
        }
    }

    /**
     * Drops all tables from the DB
     */
    public void dropTables() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs;
            String[] tables = new String[]{"Medals", "Olympics", "Events", "Athletes"};
            for (String table :
                    tables) {
                rs = stmt.executeQuery(
                        "SELECT COUNT(*) " +
                                "FROM information_schema.tables " +
                                "WHERE table_schema = 'z5414201' " +
                                "AND table_name = '" + table + "' " +
                                "LIMIT 1;");
                rs.absolute(1);
                if (rs.getInt(1) == 1)
                    stmt.executeUpdate("DROP TABLE " + table + ";");
            }
            logger.info("All tables dropped!");
        } catch (SQLException e) {
            logger.warning("Unable to drop tables. Error: " + e.getMessage());
            throw new RuntimeException("Unable to drop all tables. " + e);
        }
    }

    /**
     * Method to transfer data from CSV files into the DB tables.
     */
    public void populateTables() {
        //this should be the first line in this method.
        long time = System.currentTimeMillis();

        //populate the tables here
        String sqlOlympics = "INSERT INTO Olympics (year, season, city) VALUES (?, ?, ?)";
        String sqlEvents = "INSERT INTO Events (sport, event) VALUES (?, ?)";
        String sqlAthletes = "INSERT INTO Athletes (name, noc, gender) VALUES (?, ?, ?)";
        String sqlMedals = "INSERT INTO Medals (olympicID, eventID, athleteID, medalColour) " +
                "VALUES ((SELECT ID FROM Olympics WHERE year=? AND season=? AND city=? LIMIT 1), " +
                "(SELECT ID FROM Events WHERE sport=? AND event=? LIMIT 1), " +
                "(SELECT ID FROM Athletes WHERE name=? AND noc=? AND gender=? LIMIT 1), ?)";

        try (PreparedStatement ps = conn.prepareStatement(sqlOlympics)) {
            readData("resources/olympics.csv", ps, this::populateTable);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            logger.severe("Unable to insert values into OLYMPICS table. Error: " + e.getMessage());
        }
        logger.info("Time to populate: " + (System.currentTimeMillis() - time) + "ms");

        try (PreparedStatement ps = conn.prepareStatement(sqlEvents)) {
            readData("resources/events.csv", ps, this::populateTable);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            logger.severe("Unable to insert values into EVENTS table. Error: " + e.getMessage());
        }
        logger.info("Time to populate: " + (System.currentTimeMillis() - time) + "ms");

        try (PreparedStatement ps = conn.prepareStatement(sqlAthletes)) {
            readData("resources/athletes.csv", ps, this::populateTable);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            logger.severe("Unable to insert values into ATHLETES table. Error: " + e.getMessage());
        }
        logger.info("Time to populate: " + (System.currentTimeMillis() - time) + "ms");

        try (PreparedStatement ps = conn.prepareStatement(sqlMedals)) {
            readData("resources/medals.csv", ps, this::populateMedals);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            logger.severe("Unable to insert values into MEDALS table. Error: " + e.getMessage());
        }

        //this should be the last line in this method
        logger.info("Time to populate: " + (System.currentTimeMillis() - time) + "ms");
    }

    /**
     * Executes the queries defined in the task sheet and prints result
     * to the console.
     */
    public void runQueries() {
        String sql = "SELECT DISTINCT COUNT(*) FROM Events WHERE sport='Athletics'";
        try (Statement stmt = conn.createStatement()) {
            ResultSet res = stmt.executeQuery(sql);
            res.absolute(1);
            System.out.println("The number of distinct events that have the sport 'Athletics'");
            System.out.println(res.getString(1));
        } catch (SQLException e) {
            logger.warning("Error executing query. Query: " + sql + "Error: " + e.getMessage());
        }

        sql = "SELECT year, season, city FROM Olympics ORDER BY year";
        try (Statement stmt = conn.createStatement()) {
            ResultSet res = stmt.executeQuery(sql);
            System.out.println("The year, season, and city for each Olympics, ordered with the earliest entries first.");
            while (res.next()) {
                System.out.println(res.getString(1) + " - " + res.getString(2) + " - " + res.getString(3));
            }
        } catch (SQLException e) {
            logger.warning("Error executing query. Query: " + sql + "Error: " + e.getMessage());
        }

        try (Statement stmt = conn.createStatement()) {
            System.out.println("The total number of each medal colour awarded to athletes from Australia (NOC: AUS) over all Olympics in the database.");
            sql = "SELECT COUNT(Medals.ID) FROM Medals INNER JOIN Athletes ON Medals.athleteID=Athletes.ID WHERE Athletes.noc='AUS' AND Medals.medalColour='Gold'";
            ResultSet res = stmt.executeQuery(sql);
            res.absolute(1);
            System.out.println("Gold: " + res.getString(1));

            sql = "SELECT COUNT(Medals.ID) FROM Medals INNER JOIN Athletes ON Medals.athleteID=Athletes.ID WHERE Athletes.noc='AUS' AND Medals.medalColour='Silver'";
            res = stmt.executeQuery(sql);
            res.absolute(1);
            System.out.println("Silver: " + res.getString(1));

            sql = "SELECT COUNT(Medals.ID) FROM Medals INNER JOIN Athletes ON Medals.athleteID=Athletes.ID WHERE Athletes.noc='AUS' AND Medals.medalColour='Bronze'";
            res = stmt.executeQuery(sql);
            res.absolute(1);
            System.out.println("Bronze: " + res.getString(1));
        } catch (SQLException e) {
            logger.warning("Error executing query. Query: " + sql + "Error: " + e.getMessage());
        }

        try (Statement stmt = conn.createStatement()) {
            sql = "SELECT Athletes.name, Olympics.year, Olympics.season FROM Athletes INNER JOIN Medals ON Athletes.ID = Medals.athleteID INNER JOIN Olympics ON Medals.olympicID = Olympics.ID WHERE Athletes.noc='IRL' AND Medals.medalColour='Silver';";
            ResultSet res = stmt.executeQuery(sql);
            System.out.println("The name of all athletes from Ireland (NOC: IRL) who won silver medals, and the year / season in which they won them.");
            while (res.next()) {
                System.out.println(res.getString(1) + " - " + res.getString(2) + " - " + res.getString(3));
            }
        } catch (SQLException e) {
            logger.warning("Error executing query. Query: " + sql + "Error: " + e.getMessage());
        }
    }

    /**
     * The helper method to load data into tables via batching prepared
     * statements.
     * @param data The column values for the DB row
     * @param ps The DB connection/Prepared Statement object
     */
    public void populateTable(String[] data, PreparedStatement ps) {
        try {
            for (int i = 0; i < data.length; i++) {
                ps.setString(i+1, data[i].replace("\"",""));
            }
            ps.addBatch();
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    /**
     * The helper method used to interface with the DB and load data
     * into the medals DB
     * @param data The column values for the DB row
     * @param ps The DB connection/Prepared Statement object
     */
    public void populateMedals(String[] data, PreparedStatement ps) {
        try {
            ps.setString(1, data[3].replace("\"", ""));
            ps.setString(2, data[4].replace("\"", ""));
            ps.setString(3, data[5].replace("\"", ""));

            ps.setString(4, data[6].replace("\"", ""));
            ps.setString(5, data[7].replace("\"", ""));

            ps.setString(6, data[0].replace("\"", ""));
            ps.setString(7, data[2].replace("\"", ""));
            ps.setString(8, data[1].replace("\"", ""));

            ps.setString(9, data[8].replace("\"", ""));
            ps.addBatch();
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    /**
     * Reads a given CSV file and then executes populate table on the given data & statement
     * @param path CSV file path
     * @param ps DB Prepared Statement object to batch SQL queries
     * @param populateTable Method to interface with DB & load data into table
     */
    public void readData(String path, PreparedStatement ps, BiConsumer<String[], PreparedStatement> populateTable) {
        try (FileInputStream inputStream = new FileInputStream(path); Scanner sc = new Scanner(inputStream, StandardCharsets.UTF_8)) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] data = line.split(",");
                populateTable.accept(data, ps);
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSet executeSQL(String sql) {
        ResultSet rs;
        try {
            Statement stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            return rs;
        } catch (SQLException e) {
            logger.warning("Unable to execute sql. Error: " + e.getMessage());
            return null;
        }
    }
}
