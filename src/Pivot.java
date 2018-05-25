
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

public class Pivot {

    private static String jdbcUrl,
            jdbcUser,
            jdbcPassword,
            columnQuery,
            mvName,
            columnPrefix,
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
            columnPrefix = props.getProperty("columnPrefix");
            pivotQuery = props.getProperty("pivotQuery");
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(jdbcUrl,jdbcUser,jdbcPassword);
            PreparedStatement getColumns = connection.prepareStatement(columnQuery);
            ArrayList<String> columns = new ArrayList<>();
            ResultSet columnsRS = getColumns.executeQuery();
            while (columnsRS.next()) {
                columns.add(columnsRS.getString(1));
            }
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

    /**
     *
     * @param columns Columns we're going to pivot
     * @apiNote This could have been done in the main class but I changed how this code works before I realized that and I'm far too lazy to change things now.
     */
    private static void iterateAndPivot(ArrayList<String> columns)
    {
        System.out.println("Processing " + mvName + " with " + columns.size() + " columns\n");
        try {
            int mvNumber = 1;
            String inColString = "";
            int counter = 0;
            for (int i = 0; i < columns.size(); i++) {
                counter++;
                String curColumn = columns.get(i).replaceAll("/[^A-Za-z0-9]/", "").replaceAll(" ", "_").replaceAll("-","_");
                if (counter < 950) {
                    inColString = inColString + ", '" + columns.get(i) + "' " + columnPrefix + StringUtils.substring(curColumn, 0, 30 - columnPrefix.length()) + "";
                } else {
                    createMv(inColString,mvNumber);
                    mvNumber++;
                    counter = 0;
                    inColString = ", '" + columns.get(i) + "' " + columnPrefix + StringUtils.substring(curColumn, 0, 30 - columnPrefix.length()) + "";
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
