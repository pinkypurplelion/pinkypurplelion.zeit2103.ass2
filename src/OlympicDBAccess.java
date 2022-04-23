package src;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.Consumer;


public class OlympicDBAccess {
    Connection conn = null;


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
            String url = "jdbc:mysql://localhost:" + forwardedPort + "/z5414201";
            conn = DriverManager.getConnection(url, databaseUsername, databasePassword);

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
        readData("resources/olympics.csv", this::populateOlympics);
        readData("resources/events.csv", this::populateEvents);
        readData("resources/athletes.csv", this::populateAthletes);

        //this should be the last line in this method
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");
    }

    public void runQueries() {
        
    }

    public void populateOlympics(String[] data) {
        String sql = "INSERT INTO OLYMPICS (YEAR, SEASON, CITY) VALUES (" + data[0] + "," + data[1] + "," + data[2] + ");";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    public void populateEvents(String[] data) {
        String sql = "INSERT INTO EVENTS (SPORT, EVENT) VALUES (" + data[0] + "," + data[1] + ");";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    public void populateAthletes(String[] data) {
        String sql = "INSERT INTO ATHLETES (NAME, NOC, GENDER) VALUES (" + data[0] + "," + data[1] + "," + data[2] + ");";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    public void populateMedals(String[] data) {
        String sql = "INSERT INTO MEDALS (OLYMPICID, EVENTID, ATHLETEID, MEDALCOLOUR) VALUES ();";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    public void readData(String path, Consumer<String[]> populateTable) {
        try (FileInputStream inputStream = new FileInputStream(path); Scanner sc = new Scanner(inputStream, StandardCharsets.UTF_8)) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                String[] data = line.split(",");
                populateTable.accept(data);
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
