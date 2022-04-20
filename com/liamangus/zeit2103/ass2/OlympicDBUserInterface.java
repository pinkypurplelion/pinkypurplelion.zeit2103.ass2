package com.liamangus.zeit2103.ass2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * ***********************************************
 *
 * Description: user interface for a the OlympicMedals database.
 *
 * (DSR Assignment 2)
 *
 * @author Kathryn Kasmarik, updated by Kyle Robert Harrison
 * @version 1.1 Feb 2022
 *
 ************************************************
 */
public class OlympicDBUserInterface {
    // Main menu constants

    private final int CONTINUE = 0;
    private final int TASK1_2 = 1;
    private final int TASK1_3 = 2;
    private final int TASK2 = 3;
    private final int TASK3 = 4;
    private final int EXIT = 5;

    // The helper class that does the work
    private OlympicDBAccess dbaccess;

    /**
     * Constructor to create a new UI
     */
    public OlympicDBUserInterface() {
        dbaccess = new OlympicDBAccess();
    }

    /**
     * Method to run the UI until user chooses the exit option.
     */
    public void runUI() {
        try {
            int option = CONTINUE;
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

            while (option != EXIT) {
                System.out.println("WELCOME TO THE OLYMPIC MEDALS DATABASE");
                System.out.println("Please select an option:");
                System.out.println("[1] Create tables");
                System.out.println("[2] Drop tables");
                System.out.println("[3] Populate tables");
                System.out.println("[4] Run Queries");
                System.out.println("[5] Exit");
                option = Integer.parseInt(in.readLine());

                switch (option) {
                    case TASK1_2:
                        dbaccess.createTables();
                        System.out.println("All tables have been created.");
                        break;
                    case TASK1_3:
						dbaccess.dropTables();
                        System.out.println("All tables have been dropped.");
                        break;
                    case TASK2:
                        dbaccess.populateTables();
                        System.out.println("All tables have been populated.");
                        break;
                    case TASK3:
                        dbaccess.runQueries();
                        break;
                    case EXIT:
                        System.out.println("Bye");
                        break;
                    default:
                        System.out.println("Please enter a valid option [1-5].");
                        break;
                }

            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    // Main method to run the program
    public static void main(String[] args) {
        OlympicDBUserInterface ui = new OlympicDBUserInterface();
        ui.runUI();
    }
}
