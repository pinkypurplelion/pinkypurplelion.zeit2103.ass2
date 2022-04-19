import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Date;
import java.util.Properties;


public class OlympicDBAccess {
    Connection conn = null;
    public OlympicDBAccess() {
        String jumpserverHost = "seitux2.adfa.unsw.edu.au";
        String jumpserverUsername = "z5414201";
        // The hostname/IP address and port, you would use on the SSH server
        // to connect to the database.
        // If the database runs on the same machine as the SSH server, use "localhost".
        String databaseHost = "localhost";
        int databasePort = 3306;
        String databaseUsername = "z5414201";
        String databasePassword = "mysqlpass";

        JSch jsch = new JSch();

        try {
            // Connect to SSH jump server (this does not show an authentication code)
            Session session = jsch.getSession(jumpserverUsername, jumpserverHost);
            session.setPassword("");

            Properties prop = new Properties();
            prop.put("StrictHostKeyChecking", "no");
            session.setConfig(prop);
            session.connect();

            // Forward randomly chosen local port through the SSH channel to database host/port
            int forwardedPort = session.setPortForwardingL(0, databaseHost, databasePort);

            // Connect to the forwarded port (the local end of the SSH tunnel)
            // If you don't use JDBC, but another database client,
            // just connect it to the localhost:forwardedPort
            String url = "jdbc:mysql://localhost:" + forwardedPort;
            conn = DriverManager.getConnection(url, databaseUsername, databasePassword);

            System.out.println("Got it!");

        } catch (Exception e) {
            throw new Error("Problem", e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    public void createTables() {
        
    }

    public void dropTables() {
       
    }

    public void populateTables() {
        //this should be the first line in this method.
        long time = System.currentTimeMillis();

        //populate the tables here
        

        //this should be the last line in this method
        System.out.println("Time to populate: " + (System.currentTimeMillis() - time) + "ms");
    }

    public void runQueries() {
        
    }

 
}
