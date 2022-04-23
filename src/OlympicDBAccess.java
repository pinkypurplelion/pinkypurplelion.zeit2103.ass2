package src;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import javax.sql.DataSource;
import javax.xml.transform.Result;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.BiConsumer;
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
        String sqlMedals = "INSERT INTO MEDALS (OLYMPICID, EVENTID, ATHLETEID, MEDALCOLOUR) VALUES (?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sqlOlympics)) {
            PreparedStatement[] stmts = new PreparedStatement[]{ps};
            readData("resources/olympics.csv", stmts, this::populateTable);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");

        try (PreparedStatement ps = conn.prepareStatement(sqlEvents)) {
            PreparedStatement[] stmts = new PreparedStatement[]{ps};
            readData("resources/events.csv", stmts, this::populateTable);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");

        try (PreparedStatement ps = conn.prepareStatement(sqlAthletes)) {
            PreparedStatement[] stmts = new PreparedStatement[]{ps};
            readData("resources/athletes.csv", stmts, this::populateTable);
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");

        try (PreparedStatement ps = conn.prepareStatement(sqlMedals)) {
            String sqlGetOID = "SELECT ID FROM OLYMPICS WHERE YEAR=? AND SEASON=? AND CITY=?";
            String sqlGetEID = "SELECT ID FROM EVENTS WHERE SPORT=? AND EVENT=?";
            String sqlGetAID = "SELECT ID FROM ATHLETES WHERE NAME=? AND NOC=? AND GENDER=?";

            PreparedStatement psOID = conn.prepareStatement(sqlGetOID);
            PreparedStatement psEID = conn.prepareStatement(sqlGetEID);
            PreparedStatement psAID = conn.prepareStatement(sqlGetAID);

            PreparedStatement[] stmts = new PreparedStatement[]{ps, psOID, psEID, psAID};
            readData("resources/medals.csv", stmts, this::populateMedals);
            ps.executeBatch();
            conn.commit();

            psOID.close();
            psEID.close();
            psAID.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //this should be the last line in this method
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");
    }

    public void runQueries() {
        
    }

    public void populateTable(String[] data, PreparedStatement[] ps) {
        try {
            for (int i = 0; i < data.length; i++) {
                ps[0].setString(i+1, data[i].replace("\"",""));
            }
            ps[0].addBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void populateMedals(String[] data, PreparedStatement[] ps) {
//        System.out.println(Arrays.toString(data));
        try (Statement stmt = conn.createStatement()) {
            ps[1].setString(1, data[3].replace("\"", ""));
            ps[1].setString(2, data[4].replace("\"", ""));
            ps[1].setString(3, data[5].replace("\"", ""));

            ps[2].setString(1, data[6].replace("\"", ""));
            ps[2].setString(2, data[7].replace("\"", ""));

            ps[3].setString(1, data[0].replace("\"", ""));
            ps[3].setString(2, data[2].replace("\"", ""));
            ps[3].setString(3, data[1].replace("\"", ""));

            ResultSet rs = ps[1].executeQuery();
            rs.absolute(1);
            String oid = rs.getString(1);

            rs = ps[2].executeQuery();
            rs.absolute(1);
            String eid = rs.getString(1);

            rs = ps[3].executeQuery();
            rs.absolute(1);
            String aid = rs.getString(1);

            rs.close();

            ps[0].setString(1, oid);
            ps[0].setString(2, eid);
            ps[0].setString(3, aid);
            ps[0].setString(4, data[8].replace("\"", ""));
            ps[0].addBatch();
        } catch (SQLException e) {
            System.out.println("error: " + e.getMessage());
        }
    }

    public void readData(String path, PreparedStatement[] ps, BiConsumer<String[], PreparedStatement[]> populateTable) {
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
