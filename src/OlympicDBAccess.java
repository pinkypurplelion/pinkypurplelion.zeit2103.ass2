package src;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.BiConsumer;


public class OlympicDBAccess {
    Connection conn;
    public OlympicDBAccess() {
        String jumpserverHost = "seitux2.adfa.unsw.edu.au";
        String jumpserverUsername = "z5414201";

        String databaseHost = "localhost";
        int databasePort = 3306;
        String databaseUsername = "z5414201";
        String databasePassword = "mysqlpass";

        JSch jsch = new JSch();

        try {
            // Connect to SSH jump server (this does not show an authentication code)
            Session session = jsch.getSession(jumpserverUsername, jumpserverHost);
            session.setPassword("2Wp5^cfgrE25agtE");

            Properties prop = new Properties();
            prop.put("StrictHostKeyChecking", "no");
            session.setConfig(prop);
            session.connect();

            // Forward randomly chosen local port through the SSH channel to database host/port
            int forwardedPort = session.setPortForwardingL(0, databaseHost, databasePort);

            // Connect to the forwarded port (the local end of the SSH tunnel)
            // If you don't use JDBC, but another database client,
            // just connect it to the localhost:forwardedPort
            String url = "jdbc:mysql://localhost:" + forwardedPort + "/z5414201?useServerPrepStmts=false&rewriteBatchedStatements=true";
            conn = DriverManager.getConnection(url, databaseUsername, databasePassword);
            conn.setAutoCommit(false);
            System.out.println("Got it!");

        } catch (Exception e) {
            throw new Error("Problem", e);
        }
    }

    public void createTables() {
        String CREATE_OLYMPICS = "CREATE TABLE OLYMPICS(" +
                "ID INT NOT NULL AUTO_INCREMENT," +
                "YEAR INT," +
                "SEASON VARCHAR(7)," +
                "CITY VARCHAR(23)," +
                "PRIMARY KEY (ID));";

        String CREATE_EVENTS = "CREATE TABLE EVENTS(" +
                "  ID INT NOT NULL AUTO_INCREMENT," +
                "  SPORT VARCHAR(26)," +
                "  EVENT VARCHAR(86)," +
                "  PRIMARY KEY (ID)" +
                ");";

        String CREATE_ATHLETES = "CREATE TABLE ATHLETES(" +
                "  ID INT NOT NULL AUTO_INCREMENT," +
                "  NAME VARCHAR(94)," +
                "  NOC CHAR(3)," +
                "  GENDER CHAR(1)," +
                "  PRIMARY KEY (ID)" +
                ");";

        String CREATE_MEDALS = "CREATE TABLE MEDALS" +
                "(" +
                "    ID        INT NOT NULL AUTO_INCREMENT," +
                "    OLYMPICID INT, FOREIGN KEY (OLYMPICID) REFERENCES OLYMPICS(ID)," +
                "    EVENTID INT, FOREIGN KEY (EVENTID) REFERENCES EVENTS(ID)," +
                "    ATHLETEID INT, FOREIGN KEY (ATHLETEID) REFERENCES ATHLETES(ID)," +
                "    MEDALCOLOUR VARCHAR(7)," +
                "    PRIMARY KEY (ID)" +
                ");";

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(CREATE_OLYMPICS);
            stmt.executeUpdate(CREATE_EVENTS);
            stmt.executeUpdate(CREATE_ATHLETES);
            stmt.executeUpdate(CREATE_MEDALS);
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }



    public void dropTables() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE MEDALS, OLYMPICS, EVENTS, ATHLETES;");
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    public void populateTables() {
        //this should be the first line in this method.
        long time = System.currentTimeMillis();

        //populate the tables here
        String sqlOlympics = "INSERT INTO OLYMPICS (YEAR, SEASON, CITY) VALUES (?, ?, ?)";
        String sqlEvents = "INSERT INTO EVENTS (SPORT, EVENT) VALUES (?, ?)";
        String sqlAthletes = "INSERT INTO ATHLETES (NAME, NOC, GENDER) VALUES (?, ?, ?)";
        String sqlMedals = "INSERT INTO MEDALS (OLYMPICID, EVENTID, ATHLETEID, MEDALCOLOUR) VALUES ((SELECT ID FROM OLYMPICS WHERE YEAR=? AND SEASON=? AND CITY=?), (SELECT ID FROM EVENTS WHERE SPORT=? AND EVENT=?), (SELECT ID FROM ATHLETES WHERE NAME=? AND NOC=? AND GENDER=?), ?)";

        try (PreparedStatement ps = conn.prepareStatement(sqlOlympics)) {
            readData("resources/olympics.csv", ps, this::populateTable);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");

        try (PreparedStatement ps = conn.prepareStatement(sqlEvents)) {
            readData("resources/events.csv", ps, this::populateTable);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");

        try (PreparedStatement ps = conn.prepareStatement(sqlAthletes)) {
            readData("resources/athletes.csv", ps, this::populateTable);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");

        try (PreparedStatement ps = conn.prepareStatement(sqlMedals)) {
            readData("resources/medals.csv", ps, this::populateMedals);
            System.out.println("statements created. Executing now");
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.out.println(e.getSQLState());
            System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");
//            throw new RuntimeException(e);
        }

        //this should be the last line in this method
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");
    }

    public void runQueries() {
        String sql = "SELECT DISTINCT COUNT(*) FROM EVENTS WHERE SPORT='Athletics'";
        try (Statement stmt = conn.createStatement()) {
            ResultSet res = stmt.executeQuery(sql);
            res.absolute(1);
            System.out.println("The number of distinct events that have the sport 'Athletics'");
            System.out.println(res.getString(1));
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }

        sql = "SELECT YEAR, SEASON, CITY FROM OLYMPICS ORDER BY YEAR";
        try (Statement stmt = conn.createStatement()) {
            ResultSet res = stmt.executeQuery(sql);
            System.out.println("The year, season, and city for each Olympics, ordered with the earliest entries first.");
            while (res.next()) {
                System.out.println(res.getString(1) + " - " + res.getString(2) + " - " + res.getString(3));
            }
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }

        try (Statement stmt = conn.createStatement()) {
            System.out.println("The total number of each medal colour awarded to athletes from Australia (NOC: AUS) over all Olympics in the database.");
            sql = "SELECT COUNT(MEDALS.ID) FROM MEDALS INNER JOIN ATHLETES ON MEDALS.ATHLETEID=ATHLETES.ID WHERE ATHLETES.NOC='AUS' AND MEDALS.MEDALCOLOUR='Gold'";
            ResultSet res = stmt.executeQuery(sql);
            res.absolute(1);
            System.out.println("Gold: " + res.getString(1));

            sql = "SELECT COUNT(MEDALS.ID) FROM MEDALS INNER JOIN ATHLETES ON MEDALS.ATHLETEID=ATHLETES.ID WHERE ATHLETES.NOC='AUS' AND MEDALS.MEDALCOLOUR='Silver'";
            res = stmt.executeQuery(sql);
            res.absolute(1);
            System.out.println("Silver: " + res.getString(1));

            sql = "SELECT COUNT(MEDALS.ID) FROM MEDALS INNER JOIN ATHLETES ON MEDALS.ATHLETEID=ATHLETES.ID WHERE ATHLETES.NOC='AUS' AND MEDALS.MEDALCOLOUR='Bronze'";
            res = stmt.executeQuery(sql);
            res.absolute(1);
            System.out.println("Bronze: " + res.getString(1));
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }

        try (Statement stmt = conn.createStatement()) {
            sql = "SELECT ATHLETES.NAME, OLYMPICS.YEAR, OLYMPICS.SEASON FROM ATHLETES INNER JOIN MEDALS ON ATHLETES.ID = MEDALS.ATHLETEID INNER JOIN OLYMPICS ON MEDALS.OLYMPICID = OLYMPICS.ID WHERE ATHLETES.NOC='IRL' AND MEDALS.MEDALCOLOUR='Silver';";
            ResultSet res = stmt.executeQuery(sql);
            System.out.println("The name of all athletes from Ireland (NOC: IRL) who won silver medals, and the year / season in which they won them..");
            while (res.next()) {
                System.out.println(res.getString(1) + " - " + res.getString(2) + " - " + res.getString(3));
            }
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

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

    public void readData(String path, PreparedStatement ps, BiConsumer<String[], PreparedStatement> populateTable) {
        int counter = 0;
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
}
