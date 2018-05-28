
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class Pivot {

    private static String jdbcUrl,
            jdbcUser,
            jdbcPassword,
            columnQuery,
            mvName,
            codeTableName,
            pivotQuery;
    private static Connection connection;

    public static void main(String[] args)
    {
        try {
            File configFile = new File("./" + args[0]);
            FileInputStream fis = new FileInputStream(configFile);
            Properties props = new Properties();
            props.load(fis);
            jdbcUrl = props.getProperty("jdbcUrl");
            jdbcUser = props.getProperty("jdbcUser").toUpperCase();
            jdbcPassword = props.getProperty("jdbcPassword");
            columnQuery = props.getProperty("columnQuery");
            mvName = props.getProperty("mvName").toUpperCase();
            pivotQuery = props.getProperty("pivotQuery");
            codeTableName = props.getProperty("codeTableName").toUpperCase();
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(jdbcUrl,jdbcUser,jdbcPassword);
            PreparedStatement getColumns = connection.prepareStatement(columnQuery);
            ArrayList<String> columns = new ArrayList<>();
            ResultSet columnsRS = getColumns.executeQuery();
            while (columnsRS.next()) {
                columns.add(columnsRS.getString(1));
            }
            insertMvDetails();
            createCodeTable(columns);
            iterateAndPivot(columns);
            columns = null;
            columnsRS = null;

            connection.close();
            connection = null;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void iterateAndPivot(ArrayList<String> columns)
    {
        System.out.println("Processing " + mvName + " with " + columns.size() + " columns\n");
        try {
            int mvNumber = 1;
            String inColString = "";
            int counter = 0;
            for (int i = 0; i < columns.size(); i++) {
                counter++;
                String curColumn = String.valueOf("C_" + String.valueOf(i));
                if (counter < 950) {
                    inColString = inColString + ", '" + columns.get(i) + "' " + curColumn + "";
                } else {
                    createMv(inColString,mvNumber);
                    mvNumber++;
                    counter = 0;
                    inColString = ", '" + columns.get(i) + "' " + curColumn + "";
                }
            }
            if ((mvNumber > 1 && columns.size() % 950 != 0) || columns.size() < 950) {
                createMv(inColString,mvNumber);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertMvDetails() {
        String insertQuery = "INSERT INTO PIVOT_REFERENCE (MVNAME,CODETABLE,PIVOT_QUERY,COLUMN_QUERY) VALUES ('"+ mvName + "','" + codeTableName + "','" + StringUtils.substring(pivotQuery.replaceAll("'","''"),0,4000) + "','" + StringUtils.substring(columnQuery.replaceAll("'","''"),0,4000) + "')";
        System.out.println(insertQuery);
        try {
            try {
                connection.createStatement().executeUpdate("CREATE TABLE PIVOT_REFERENCE (MVNAME VARCHAR2(30), CODETABLE VARCHAR2(30), PIVOT_QUERY VARCHAR2(4000), COLUMN_QUERY VARCHAR2(4000))");
                connection.createStatement().executeUpdate(insertQuery);
                System.out.println("Created PIVOT_REFERENCE table");
            }
            catch (Exception sqle) {
                int exists = 0;
                ResultSet res = connection.prepareStatement("SELECT COUNT(MVNAME) FROM PIVOT_REFERENCE WHERE UPPER(MVNAME) = '" + mvName + "'").executeQuery();
                res.next();
                exists = res.getInt(1);
                res.close();
                if (exists > 0) {
                    System.out.println("PIVOT_REFERENCE record for " + mvName + " already exists. DELETING");
                    connection.createStatement().executeUpdate("DELETE FROM PIVOT_REFERENCE WHERE UPPER(MVNAME) = '"+mvName+"'");
                    System.out.println("Inserting PIVOT_REFERENCE record for " + mvName);
                    connection.createStatement().executeUpdate(insertQuery);
                    System.out.println("Inserted");
                }
                else {
                    System.out.println("Inserting PIVOT_REFERENCE record for " + mvName);
                    connection.createStatement().executeUpdate(insertQuery);
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }
    private static void createCodeTable(ArrayList<String> columns) {
        try {
            try {
                connection.createStatement().executeUpdate("DROP TABLE " + codeTableName);
            }
            catch (Exception sqle) {
                //Do nothing, the table might not exist
                System.out.println(codeTableName + " table doesn't exist, creating");
            }
            String tableCreate = "CREATE TABLE " + codeTableName + " (CODE NUMBER, COLUMN_NAME VARCHAR2(4000))";
            connection.createStatement().executeUpdate(tableCreate);
            Statement batchInsert = connection.createStatement();
            for (int i=0;i<columns.size();i++) {
                String insert = "INSERT INTO " + codeTableName + " (CODE,COLUMN_NAME) VALUES (" + String.valueOf(i) + ",'" + StringUtils.substring(columns.get(i),0, 4000) + "')";
                batchInsert.addBatch(insert);
            }
            System.out.println("Inserting batch codes for " + codeTableName);
            batchInsert.executeBatch();
            batchInsert.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void createMv(String inColString,int mvNumber)
    {
        try {
            String pivot = pivotQuery + StringUtils.substring(inColString, 2, inColString.length()) + "))";
            String mvNameNumber = mvName + "_" + String.valueOf(mvNumber);
            String mvCreate = "CREATE MATERIALIZED VIEW " + mvNameNumber + " NOLOGGING CACHE BUILD IMMEDIATE AS " + pivot;
            String checkForView = "SELECT COUNT(*) FROM ALL_MVIEWS WHERE MVIEW_NAME='" + mvNameNumber + "' AND OWNER='" +jdbcUser + "'";
            ResultSet res =  connection.prepareStatement(checkForView).executeQuery();
            res.next();
            int doesExist = res.getInt(1);
            res.close();
            if (doesExist >= 1) {
                String removeView = "DROP MATERIALIZED VIEW " + mvNameNumber;
                System.out.println(mvNameNumber + " already exists. DROPPING.");
                connection.createStatement().executeUpdate(removeView);
            }

            System.out.println(mvCreate);
            connection.createStatement().executeUpdate(mvCreate);
            System.out.println("Creating " + mvName + "_" + mvNumber + "\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}