
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

    public static void main(String[] args)
    {
        try {
        File configFile = new File("./" + args[0]);
        FileInputStream fis = new FileInputStream(configFile);
        Properties props = new Properties();
        props.load(fis);
        String jdbcUrl = props.getProperty("jdbcUrl"),
               jdbcUser = props.getProperty("jdbcUser"),
               jdbcPassword = props.getProperty("jdbcPassword"),
               columnQuery = props.getProperty("columnQuery"),
               mvName = props.getProperty("mvName"),
               columnPrefix = props.getProperty("columnPrefix"),
               pivotQuery = props.getProperty("pivotQuery");
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connection = DriverManager.getConnection(jdbcUrl,jdbcUser,jdbcPassword);
        PreparedStatement getColumns = connection.prepareStatement(columnQuery);
        ArrayList<String> columns = new ArrayList<>();
        ResultSet columnsRS = getColumns.executeQuery();
        while (columnsRS.next()) {
            columns.add(columnsRS.getString(1));
        }
        iterateAndPivot(columns,columnPrefix,pivotQuery,mvName,connection);
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
     * @param columnPrefix Prefix in front of the column EX: MYPREFIX_ATTRIBUTE1
     * @param pivotQuery Pivot you want to perform minus the IN statement (see example properties)
     * @param mvName Materalized view(s) that will be the output of the pivot
     * @param connection DB Connection
     * @apiNote This could have been done in the main class but I changed how this code works before I realized that and I'm far too lazy to change things now.
     */
    private static void iterateAndPivot(ArrayList<String> columns,String columnPrefix,String pivotQuery, String mvName, Connection connection)
    {
        System.out.println("Processing " + mvName + " with " + columns.size() + " columns");
        try {
            int mvNumber = 1;
            String inColString = "";
            int counter = 0;
            for (int i = 0; i < columns.size(); i++) {
                counter++;
                String curColumn = columns.get(i).replaceAll("/[^A-Za-z0-9]/", "").replaceAll(" ", "_");
                if (counter < 950) {
                    inColString = inColString + ", '" + curColumn + "' " + columnPrefix + StringUtils.substring(curColumn, 0, 30 - columnPrefix.length()) + "";
                } else {
                    String pivot = pivotQuery + StringUtils.substring(inColString, 2, inColString.length()) + "))";
                    String mvCreate = "CREATE MATERIALIZED VIEW " + mvName + "_" + String.valueOf(mvNumber) + " NOLOGGING CACHE BUILD IMMEDIATE AS " + pivot;
                    connection.createStatement().executeUpdate(mvCreate);
                    System.out.println("Creating " + mvName + "_" + mvNumber);
                    mvNumber++;
                    counter = 0;
                    inColString = ", '" + curColumn + "' " + columnPrefix + StringUtils.substring(curColumn, 0, 30 - columnPrefix.length()) + "";

                }
            }
            if ((mvNumber > 1 && columns.size() % 950 != 0) || columns.size() < 950) {
                String pivot = pivotQuery + StringUtils.substring(inColString, 2, inColString.length()) + "))";
                String mvCreate = "CREATE MATERIALIZED VIEW " + mvName + "_" + String.valueOf(mvNumber) + " NOLOGGING CACHE BUILD IMMEDIATE AS " + pivot;
                connection.createStatement().executeUpdate(mvCreate);
                System.out.println("Creating " + mvName + "_" + mvNumber);

            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
