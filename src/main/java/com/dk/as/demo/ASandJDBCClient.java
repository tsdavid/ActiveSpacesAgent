package com.dk.as.demo;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.tibco.datagrid.BatchResult;
import com.tibco.datagrid.DataGrid;
import com.tibco.datagrid.DataGridException;
import com.tibco.datagrid.DataGridRuntimeException;
import com.tibco.datagrid.Row;
import com.tibco.datagrid.RowSet;
import com.tibco.datagrid.Session;
import com.tibco.datagrid.Table;
import com.tibco.datagrid.TibDateTime;
import com.tibco.datagrid.jdbc.ConnectionImpl;
import com.tibco.datagrid.jdbc.ResultSetImplDG;

/*
 * ASandJDBCClient is an example client program which can work with any user table. Options
 * are provided which allow you to use features of the AS Java API as well as features of
 * the JDBC driver interface. While using this client you can switch the table the AS Java
 * API should act upon, use just the JDBC driver interface and SQL commands, or use
 * a combination of both AS Java API methods and JDBC driver methods for performing actions.
 * 
 * This example uses the ActiveSpaces Java API for:
 *     putting data into a table
 *     getting data from a table
 *     deleting data from a table
 *     iterating over data in a table
 *     
 * This example uses the JDBC API for:
 *     connecting to a data grid
 *     creating tables and indexes using SQL DDL commands
 *     dropping tables and indexes using SQL DDL commands
 *     static queries using the java.sql.Statement interface
 *     parameterized queries using the java.sql.PreparedStatement interface
 *     getting and displaying data grid and table meta data
 *     discovering the columns for putting data into a table
 *     discovering the columns of a primary key for getting or deleting data from a table
 *     
 * Running The Example
 * -------------------
 * Out-of-the-box this example will connect to a data grid named _default and where the
 * Realm Server has been started using the default Realm URL of http://localhost:8080.
 * The ActiveSpaces JDBC driver is shipped as part of the ActiveSpaces Java API and is
 * included in the file tibdg.jar.
 * 
 * Prior to running this example, you should have started your data grid. See the
 * sample script as-start. Running the as-start script without any command line options
 * will configure a data grid named '_default' and start up the data grid processes
 * using a Realm URL of 'http://localhost:8080'.
 * 
 * Follow the instructions in README.md in the Java samples directory for setting up
 * your environment to build and run the ActiveSpaces Java samples. Then run:
 *     java com.tibco.datagrid.samples.ASandJDBCClient
 *
 * JDBC Connection
 * ---------------
 * The JDBC Connection to the data grid is made in the method initializeJDBC().
 * To connect to a data grid using the AS JDBC driver interface, you need to know two things:
 *     (1) name of the ActiveSpaces Driver - com.tibco.datagrid.jdbc.DriverImpl
 *     (2) ActiveSpaces JDBC URL - jdbc:tibco:tibdg[:grid_name][;&ltproperty_name&gt=&ltvalue&gt]...
 *
 * If you specify the JDBC URL without specifying a grid name, the default grid name of
 * "_default" will be used. If you specify the JDBC URL without specifying a realm URL,
 * the default URL of "http://localhost:8080" will be used. These default settings are
 * supplied by the JDBC driver itself. However, this example also uses the defaults for
 * handling this example's command line parameter settings.
 * 
 * The property names are the string associated with the datagrid.Connection properties such as:
 *     TIBDG_CONNECTION_PROPERTY_STRING_REALMURL = "com.tibco.tibdg.realmurl"
 * You can specify the full property string or the part of the string without the
 * "com.tibco.tibdg" prefix. For example:
 *     jdbc:tibco:tibdg:mygrid;realmurl=http://localhost:6778
 * 
 * Tables
 * ------
 * This example does *not* have a default table to use. You can define a table
 * within the example and then use the table or use any table that has already
 * been configured in your data grid.
 * 
 * To define a new table, the 'u' (update) or 'ps' (PreparedStatement) options can be
 * used. These options can take the following SQL DDL commands:
 *     CREATE TABLE
 *     CREATE INDEX
 *     DROP INDEX
 *     DROP TABLE
 * 
 * To set the table for future operations to act upon, use the 't' option. It is required
 * to set the "current table" prior to using the options which put, get or delete rows
 * from a table. The table you specify as the current table must be already defined
 * in the data grid. The current table can be changed at any time by using the 't' option.
 * 
 * Meta Data
 * ---------
 * Selecting the 'md' option in this example will allow you to see information about
 * the data grid and the tables which have been defined. The as-start script defines
 * the table 't1' for you to start with. After starting this sample and selecting
 * the 'md' option to display grid meta data, you should see information about
 * table 't1' displayed. If you create additional tables using this example, selecting
 * the 'md' option to display grid meta data again should now display information about
 * your new tables also.
 * 
 * Populating a Table
 * ------------------
 * Prior to putting data into a table, the table to use must first be set using the 't'
 * option. Then use the 'p' option to put a row of data into the table. You will be
 * prompted to enter values for each column of the row. Just hitting <Enter> for a value
 * will skip putting a value into that column. Once values for all columns have been set,
 * the row will be put into the table.
 * 
 * Getting or Deleting Table Rows
 * ------------------------------
 * The 'g' and 'd' options work similar to the 'p' option for getting and deleting rows
 * of data from a table (respectively). The table to act upon must first have been set
 * using the 't' option. For the 'g' and 'd' options you will be prompted to enter the
 * values for the primary key column(s) of the row to get or delete.
 * 
 * PreparedStatements
 * ------------------
 * This example contains support for creating a PreparedStatement and executing either
 * SELECT statements or DDL statements using the PreparedStatement. SELECT statements with
 * parameters is also supported.
 * 
 * Batching
 * --------
 * This example contains several options which demonstrate the use of batching with
 * ActiveSpaces. These options are: pm (put multiple), gm (get multiple), and dm
 * (delete multiple). To use these options, the table for these options to act upon
 * must first be set with the 't' option.
 * 
 * All of these options will only work:
 *     - With a non-transacted session
 *     - With tables which have primary keys consisting of a single column
 * 
 * The 'pm' option has the additional constraint that the table must contain one column
 * of type string/VARCHAR which is a non-primary key column. See the comments in the
 * method handlePutMultiple() to see more information about how multiple rows are
 * created and sent to the data grid.
 * 
 * The 'pm' option works across multiple tables. With the 'pm' option you:
 *     First create an array to hold all of the rows you want to send to the data grid.
 *     Then specify the number of rows to create for the current table,
 *     Optionally, exit the option and set a new table. Then re-enter the option
 *     and add new rows for the new table. This can be done as many times as you
 *     like until the capacity of the array of rows is reached.
 *     Then send the array of rows to the data grid.
 * 
 * The 'gm' and 'dm' options work on a single table. An array of rows which contain
 * the primary keys to act upon is created and sent to the data grid in batches.
 * 
 */
public class ASandJDBCClient
{
    // connection related defaults
    public static final String DEFAULT_REALMURL = "http://localhost:8080";
    public static final String DEFAULT_GRID_NAME = "_default";
    
    // batching related defaults
    public static final int DEFAULT_OP_COUNT = 10000;
    public static final int DEFAULT_BATCH_SIZE = 1024;
    
    // The user can specify a JDBC URL to use or one can be put
    // together by the other command line parameters specified.
    // Data grid properties specified on the command line will
    // override the same property specified in the JDBC URL.
    // Realm URL and grid name in the JDBC URL cannot be overridden.
    String jdbcURL = null;

    // use -r command line option to change the Realm URL
    String realmurl = DEFAULT_REALMURL;
    
    // use the -gridName command line option to change the name of the
    // data grid to connect to
    String gridName = DEFAULT_GRID_NAME;
    
    // other settings which can be overridden by command line options
    double connection_timeout = 20;
    boolean dgTxnSession = false;
    String username = null;
    String password = null;
    String trustFileName = null;
    boolean trustAll = false;
    String chkptName = null;
    String consistency = null;
    String outputFileName = null;
    int    outputFileCount = 0;
    long   stmtPrefetch = 0;
    
    public void usage()
    {
        System.out.printf("Usage: java %s [options] %n", this.getClass().getCanonicalName());
        System.out.printf("Options:%n");
        System.out.printf("\t-h or -help       display command line options.%n");
        System.out.printf("\t-jdbcurl          JDBC URL to use for connecting. %n");
        System.out.printf("\t-r                Url of the realm server (Default %s) %n", DEFAULT_REALMURL);
        System.out.printf("\t-gridName         name of a preconfigured data grid (Default: _default)%n");
        System.out.printf("\t-timeout          Connection timeout value (Default 20 sec)%n");
        System.out.printf("\t-txn              Create a transacted session. (Default false)%n");
        System.out.printf("\t-username         username%n");
        System.out.printf("\t-password         password%n");
        System.out.printf("\t-trust-file       Realm Server PEM trust file, incompatible with -trust-all%n");
        System.out.printf("\t-trust-all        Do not verify identity of realm servers%n");
        System.out.printf("\t-loglevel         debug, info, off, severe, verbose, warn (Default: info)%n");
        System.out.printf("\t-checkpoint       name of a checkpoint to run against%n");
        System.out.printf("\t-consistency      statement consistency (snapshot | global)%n");
        System.out.printf("\t-output-file      store binary data from columns into files prefixed with this name%n");
        System.out.printf("\t-prefetch         number of rows to prefetch for each query%n");
    }

    public ASandJDBCClient() { }
    
    public void printSQLException(SQLException sqlex)
    {
        Iterator <Throwable> it = sqlex.iterator();
        while (it.hasNext())
        {
            Throwable ex = it.next();
            System.out.println(ex.getMessage());
        }
    }
    
    public void run() throws DataGridException, SQLException
    {
        // get ActiveSpaces Connection and Session properties
        // from default settings or parsed command line settings
        Properties props = getProperties();

        // connect to ActiveSpaces using JDBC
        // try-with-resources ensures jdbcConnection is automatically closed at the end
        try (Connection jdbcConnection = initializeJDBC(props))
        {
            System.out.println("\nConnected to data grid: " + gridName);
            doWork(jdbcConnection, props);
        }
        catch (SQLException sqlex)
        {
            printSQLException(sqlex);
        }
    }
    
    public static void main(String[] args)
    {
        System.out.println("ActiveSpaces Example: ASandJDBCClient");
        try
        {
            ASandJDBCClient client = new ASandJDBCClient();
            boolean runClient = true;
            try
            {
                client.parseArgs(args);
            }
            catch (IllegalArgumentException pex)
            {
                // there was an error parsing the command-line arguments
                if (pex.getMessage() != null)
                    System.out.println(pex.getMessage());
                client.usage();
                runClient = false;
            }
            if (runClient)
            {
                client.run();
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace(System.err);
        }
    }

    public void parseArgs(String[] args) throws IllegalArgumentException
    {
        int argc = args.length;
        for (int i = 0; i < args.length; i++)
        {
            String flag = args[i];
            if (flag.startsWith("--"))
            {
                // allow both '-' and '--'
                flag = flag.substring(1);
            }

            if (flag.equalsIgnoreCase("-h") || flag.equalsIgnoreCase("-help"))
            {
                // throw exception with empty reason so usage will get displayed by caller
                throw new IllegalArgumentException();
            }
            else if (flag.equalsIgnoreCase("-jdbcurl") && i < argc - 1)
            {
                jdbcURL = args[++i];
            }
            else if (flag.equalsIgnoreCase("-r") && i < argc - 1)
            {
                realmurl = args[++i];
            }
            else if (flag.equalsIgnoreCase("-timeout") && i < argc - 1)
            {
                try
                {
                    connection_timeout = Double.valueOf(args[++i]);
                }
                catch (NumberFormatException ex)
                {
                    String errMsg = "Invalid value for command line argument '" + args[i-1] + "', value: '" + args[i] + "'";
                    throw new IllegalArgumentException(errMsg);
                }
            }
            else if (flag.equalsIgnoreCase("-txn") && i < argc - 1)
            {
                // allow data grid specific operations to be in a transacted session
                dgTxnSession = Boolean.valueOf(args[++i]);
            }
            else if (flag.equalsIgnoreCase("-gridName") && i < argc - 1)
            {
                gridName = args[++i];
            }
            else if (flag.equalsIgnoreCase("-username") && i < argc - 1)
            {
                username = args[++i];
            }
            else if (flag.equalsIgnoreCase("-password") && i < argc - 1)
            {
                password = args[++i];
            }
            else if (flag.equalsIgnoreCase("-trust-all"))
            {
                trustAll = true;
            }
            else if (flag.equalsIgnoreCase("-trust-file") && i < argc - 1)
            {
                trustFileName = args[++i];
            }
            else if (flag.equalsIgnoreCase("-loglevel") && i < argc - 1)
            {
                try
                {
                    setLogLevel(args[++i]);
                }
                catch (Exception ex)
                {
                    String errMsg = "Failed to set log level '" + args[i] + ", reason: " + ex.getMessage();
                    throw new IllegalArgumentException(errMsg);
                }
            }
            else if (flag.equalsIgnoreCase("-checkpoint") && i < argc - 1)
            {
                chkptName = args[++i];
            }
            else if (flag.equalsIgnoreCase("-consistency") && i < argc - 1)
            {
                consistency = args[++i];
            }
            else if (flag.equalsIgnoreCase("-output-file") && i < argc - 1)
            {
                outputFileName = args[++i];
            }
            else if (flag.equalsIgnoreCase("-prefetch") && i < argc - 1)
            {
                try
                {
                    stmtPrefetch = Long.valueOf(args[++i]);
                }
                catch (NumberFormatException ex)
                {
                    String errMsg = "Invalid value for command line argument '" + args[i-1] + "', value: '" + args[i] + "'";
                    throw new IllegalArgumentException(errMsg);
                }
            }
            else
            {
                String errMsg = "Invalid command line argument or the value is missing for '" + args[i] + "'";
                throw new IllegalArgumentException(errMsg);
            }
        }
        if (trustAll && trustFileName != null)
        {
            String errMsg = "Setting -trust-all is incompatible with providing a trust file in -trust-file";
            throw new IllegalArgumentException(errMsg);
        }
        if (consistency != null)
        {
            if (!consistency.isEmpty() && !consistency.equalsIgnoreCase("snapshot")
                    && !consistency.equalsIgnoreCase("global"))
            {
                String errMsg = "Invalid consistency setting: " + consistency;
                throw new IllegalArgumentException(errMsg);
            }
        }
    }

    public void setLogLevel(String logArg) throws Exception
    {
        String logLevel;
        switch (logArg.toLowerCase())
        {
        case "debug":
            logLevel = DataGrid.TIB_LOG_LEVEL_DEBUG;
            break;
        case "info":
            logLevel = DataGrid.TIB_LOG_LEVEL_INFO;
            break;
        case "off":
            logLevel = DataGrid.TIB_LOG_LEVEL_OFF;
            break;
        case "severe":
            logLevel = DataGrid.TIB_LOG_LEVEL_SEVERE;
            break;
        case "verbose":
            logLevel = DataGrid.TIB_LOG_LEVEL_VERBOSE;
            break;
        case "warn":
            logLevel = DataGrid.TIB_LOG_LEVEL_WARN;
            break;
        default:
            logLevel = logArg;
        }

        DataGrid.setLogLevel(logLevel);
    }
    
    public Properties getProperties()
    {
        // sets the datagrid.Connection and datagrid.Session properties
        // parsed command line options override default property settings
        Properties props = new Properties();
        props.setProperty(com.tibco.datagrid.Connection.TIBDG_CONNECTION_PROPERTY_DOUBLE_TIMEOUT, String.valueOf(connection_timeout));
        props.setProperty(Session.TIBDG_SESSION_PROPERTY_BOOLEAN_TRANSACTED, String.valueOf(dgTxnSession));

        if (username != null && password != null)
        {
            props.setProperty(com.tibco.datagrid.Connection.TIBDG_CONNECTION_PROPERTY_STRING_USERNAME, username);
            props.setProperty(com.tibco.datagrid.Connection.TIBDG_CONNECTION_PROPERTY_STRING_USERPASSWORD, password);
        }

        boolean secureRealm = realmurl.toLowerCase().startsWith("https://");

        if (secureRealm)
        {
            if (trustAll)
            {
                props.setProperty(com.tibco.datagrid.Connection.TIBDG_CONNECTION_PROPERTY_STRING_TRUST_TYPE,
                                  com.tibco.datagrid.Connection.TIBDG_CONNECTION_HTTPS_CONNECTION_TRUST_EVERYONE);
            }
            else if (trustFileName != null)
            {
                props.setProperty(com.tibco.datagrid.Connection.TIBDG_CONNECTION_PROPERTY_STRING_TRUST_TYPE,
                                  com.tibco.datagrid.Connection.TIBDG_CONNECTION_HTTPS_CONNECTION_USE_SPECIFIED_TRUST_FILE);
                props.setProperty(com.tibco.datagrid.Connection.TIBDG_CONNECTION_PROPERTY_STRING_TRUST_FILE,
                                  trustFileName);
            }
        }
        
        if (chkptName != null && !chkptName.isEmpty())
        {
            // set up the properties so that all reads from the data grid
            // will use the specified checkpoint
            System.out.println("Setting checkpoint: " + chkptName);
            // set up to retrieve the grid and table metadata using the checkpoint
            props.setProperty(com.tibco.datagrid.GridMetadata.TIBDG_GRIDMETADATA_PROPERTY_STRING_CHECKPOINT_NAME, chkptName);
            // for checkpoints, always use snapshot consistency as they are already globally consistent
            props.setProperty(com.tibco.datagrid.Session.TIBDG_SESSION_PROPERTY_STRING_CHECKPOINT_NAME, chkptName);
            props.setProperty(com.tibco.datagrid.Statement.TIBDG_STATEMENT_PROPERTY_STRING_CONSISTENCY, com.tibco.datagrid.Statement.TIBDG_STATEMENT_CONSISTENCY_SNAPSHOT);
        }
        if (consistency != null && !consistency.isEmpty())
        {
            if (consistency.equalsIgnoreCase("global"))
            {
                props.setProperty(com.tibco.datagrid.Statement.TIBDG_STATEMENT_PROPERTY_STRING_CONSISTENCY, com.tibco.datagrid.Statement.TIBDG_STATEMENT_CONSISTENCY_GLOBAL_SNAPSHOT);
            }
            else if (consistency.equalsIgnoreCase("snapshot"))
            {
                props.setProperty(com.tibco.datagrid.Statement.TIBDG_STATEMENT_PROPERTY_STRING_CONSISTENCY, com.tibco.datagrid.Statement.TIBDG_STATEMENT_CONSISTENCY_SNAPSHOT);
            }
        }
        if (stmtPrefetch != 0)
        {
            props.put(com.tibco.datagrid.Statement.TIBDG_STATEMENT_PROPERTY_LONG_PREFETCH, stmtPrefetch);
        }
        return props;
    }

    public Connection initializeJDBC (Properties props) throws SQLException
    {
        // The ActiveSpaces jar file contains META-INF/services/java.sql.Driver
        // which allows the JDBC DriverManager to find the implementation class of
        // the ActiveSpaces implementation of the java.sql.Driver interface. It is
        // no longer necessary to register the AS JDBC driver from your application.
        //
        // To use the ActiveSpaces JDBC driver you only need to know one thing:
        //     The ActiveSpaces JDBC URL
        //
        // We are just using a simple form of the JDBC URL and passing any other settings
        // as properties to keep the code simple. But we could have also added all of the
        // properties onto the JDBC URL. The ActiveSpaces JDBC URL has the following form:
        //     jdbc:tibco:tibdg[:grid_name][;<property_name>=<value>]...
        //
        // The property names are the string associated with the datagrid.Connection
        // properties such as:
        //     TIBDG_CONNECTION_PROPERTY_DOUBLE_CONNECT_WAIT_TIME = "com.tibco.tibdg.connectwaittime"
        // You can specify the full property string or the part of the string without
        // the "com.tibco.tibdg" prefix. For example:
        //     jdbc:tibco:tibdg:mygrid;connectwaittime=0.5
        //
        // If you specify the JDBC URL without specifying a grid name, the default grid
        // name of "_default" will be used. If you specify the JDBC URL without specifying
        // a realm URL, the default URL of "http://localhost:8080" will be used. These
        // default settings are supplied by the JDBC driver itself.
        //
        // Any unrecognized properties are ignored.
        //
        if (jdbcURL == null)
        {
            jdbcURL          = "jdbc:tibco:tibdg:" + gridName + ";realmurl=" + realmurl;
        }

        // Establish a JDBC connection to the data grid
        Connection jdbcConnection = DriverManager.getConnection(jdbcURL, props);

        return jdbcConnection;
    }

    public void doWork(Connection jdbcConnection, Properties props) throws DataGridException, SQLException
    {
        // doWork implements a loop which prompts the user to select what they
        // want to do with the data grid and then performs the selected action
        
        // AS data grid objects which will need to get cleaned up at the end
        com.tibco.datagrid.Session dgSession = null;
        
        // JDBC objects we should ensure get cleaned up at the end
        PreparedStatement currentPreparedStatement = null;
        
        try
        {        
            // Get the DatabaseMetaData so that we can discover the characteristics
            // of the data grid and its tables. We only need to get a DatabaseMetaData
            // object once as it will dynamically get the most up-to-date info as
            // needed.
            DatabaseMetaData jdbcMetadata = jdbcConnection.getMetaData();
    
            // Call the custom method ConnectionImpl.getDataGridConnection() to get
            // access to the ActiveSpaces datagrid.Connection object held by the 
            // JDBC Connection to ActiveSpaces. Use datagrid.Connection to create
            // an ActiveSpaces Session object for those options which don't use JDBC calls.
            // Note: The jdbcConnection object owns the datagrid.Connection object
            //       and is responsible for cleaning it up.
            if (jdbcConnection.isWrapperFor(com.tibco.datagrid.jdbc.ConnectionImpl.class))
            {
                ConnectionImpl connectionImpl = jdbcConnection.unwrap(com.tibco.datagrid.jdbc.ConnectionImpl.class);
                if (connectionImpl == null)
                {
                    throw new SQLException("Invalid ActiveSpaces JDBC Connection");
                }
                com.tibco.datagrid.Connection dgConnection = connectionImpl.getDataGridConnection();
                dgSession = dgConnection.createSession(props);
            }
            else
            {
                 throw new SQLException("Invalid ActiveSpaces JDBC Connection");
            }           

            helpMenu(dgTxnSession);

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            String input = "";

            do
            {
                if (dgTxnSession)
                {
                    // include txn related commands in menu of options
                    System.out.println("** This is a transacted session **");
                    System.out.print("\nMain: [t/p/g/d/i/c/r/s/u/ps/md/h/q]: ");
                }
                else
                {
                    System.out.print("\nMain: [t/p/pm/g/gm/d/dm/i/s/u/ps/md/h/q]: ");
                }
                
                input = readInput(br);
                if (input == null)
                {
                    continue;
                }

                if (input.equals("t"))
                {
                    // The 't' option is used to specify the table to use for any further
                    // non-SQL operations (e.g. put, get). Setting a new table will
                    // automatically close the last table that was set. So it is not really
                    // necessary to close the last table before setting a new table. But
                    // maybe you don't want to have an open table, if you are only going
                    // to be doing SQL commands from this point on.
                    setCurrentTable(dgSession, props);
                }
                else if (input.equals("p"))
                {
                    Table table = getCurrentTable();
                    if (table == null)
                    {
                        System.out.println("Set the table using the 't' option prior to invoking the 'p' option\n");
                        continue;
                    }
    
                    Row putRow = populateRow(jdbcMetadata);
                    if (putRow != null)
                    {
                        try
                        {
                            table.put(putRow);
                            System.out.println("Put: Success");
                        }
                        catch (DataGridException dataGridException)
                        {
                            System.err.println("Put: Failed to put row into table. " + dataGridException.getMessage());
                        }
                        catch (DataGridRuntimeException rtException)
                        {
                            System.err.println("Put: Failed to put row into table. " + rtException.getMessage());
                        }
                        finally
                        {
                            try
                            {
                                putRow.destroy();
                            }
                            catch (DataGridException e)
                            {
                                e.printStackTrace(System.err);
                            }
                        }
                    }
                }
                else if (input.equals("pm"))
                {
                    if (getCurrentTable() == null)
                    {
                        System.out.println("Set the table using the 't' option prior to invoking the 'pm' option\n");
                        continue;
                    }
                    
                    if (dgTxnSession)
                    {
                        System.out.println("Put Multiple is not allowed for transacted sessions.");
                        continue;
                    }
                    
                    handlePutMultiple(jdbcMetadata, dgSession);
                }
                else if (input.equals("g"))
                {
                    Table table = getCurrentTable();
                    if (table == null)
                    {
                        System.out.println("Set the table using the 't' option prior to invoking the 'p' option\n");
                        continue;
                    }
    
                    Row keyRow = populatePrimaryKeyRow(jdbcMetadata);
                    if (keyRow != null)
                    {   
                        try
                        {
                            Row getRow = table.get(keyRow);
                            if (getRow != null)
                            {
                                System.out.println("Get: \n    " + getRow);
                                getRow.destroy();
                            }
                            else
                            {
                                System.out.println("Get: Row does not exist");
                            }
                        }
                        catch (DataGridException dataGridException)
                        {
                            System.err.println("Get: Failed to retrieve row from table. " + dataGridException.getMessage());
                        }
                        catch (DataGridRuntimeException rtException)
                        {
                            System.err.println("Get: Failed to retrieve row from table. " + rtException.getMessage());
                        }
                        finally
                        {
                            try
                            {
                                keyRow.destroy();
                            }
                            catch (DataGridException e)
                            {
                                e.printStackTrace(System.err);
                            }
                        }                   
                    }
                }
                else if (input.equals("gm"))
                {
                    Table table = getCurrentTable();
                    if (table == null)
                    {
                        System.out.println("Set the table using the 't' option prior to invoking the 'p' option\n");
                        continue;
                    }
                    
                    if (dgTxnSession)
                    {
                        System.out.println("Get Multiple is not allowed for transacted sessions.");
                        continue;
                    }
                    
                    handleGetMultiple(jdbcMetadata, dgSession);
                }
                else if (input.equals("d"))
                {
                    Table table = getCurrentTable();
                    if (table == null)
                    {
                        System.out.println("Set the table using the 't' option prior to invoking the 'p' option\n");
                        continue;
                    }
                    
                    Row keyRow = populatePrimaryKeyRow(jdbcMetadata);
                    if (keyRow != null)
                    {   
                        try
                        {
                            table.delete(keyRow);
                            System.out.println("Delete: Success");
                        }
                        catch (DataGridException dataGridException)
                        {
                            System.err.println("Delete: Failed to delete row from table. " + dataGridException.getMessage());
                        }
                        catch (DataGridRuntimeException rtException)
                        {
                            System.err.println("Delete: Failed to delete row from table. " + rtException.getMessage());
                        }
                        finally
                        {
                            try
                            {
                                keyRow.destroy();
                            }
                            catch (DataGridException e)
                            {
                                e.printStackTrace(System.err);
                            }
                        }
                    }
                }
                else if (input.equals("dm"))
                {
                    Table table = getCurrentTable();
                    if (table == null)
                    {
                        System.out.println("Set the table using the 't' option prior to invoking the 'p' option\n");
                        continue;
                    }
                    
                    if (dgTxnSession)
                    {
                        System.out.println("Delete Multiple is not allowed for transacted sessions.");
                        continue;
                    }
                    
                    handleDeleteMultiple(jdbcMetadata, dgSession);
                }
                else if (input.equals("i"))
                {
                    if (dgCurrentTable == null)
                    {
                        System.out.println("Set the table using the 't' option prior to invoking the 'p' option\n");
                        continue;
                    }
                    
                    System.out.print("Iterate: Enter the filter (string): ");
                    String filter = readValue(br);
                    if (filter == null)
                    {
                        System.out.println("\tUsing null filter");
                    }
                    else
                    {
                        System.out.println("\tUsing filter: " + filter);
                    }
      
                    try (RowSet rowSet = dgCurrentTable.createRowSet(filter, null))
                    {
                        int rowCount = 0;
                        for (Row itrRow : rowSet)
                        {
                            rowCount++;
                            try
                            {
                                System.out.println("Row from iterator: " + itrRow);
                            }
                            finally
                            {
                                itrRow.destroy();
                            }
                        }
                        System.out.printf("Iterated over %d rows in the table %n", rowCount);
                    }
                }
                else if (input.equals("c"))
                {
                    // currently transactions are only supported for non-JDBC operations (e.g. put, get, delete)
                    if (!dgTxnSession)
                    {
                        System.out.println("Commit is not supported for a non-transacted session ");
                        continue;
                    }

                    try
                    {
                        dgSession.commit();
                    }
                    catch (DataGridException e)
                    {
                        System.out.println("Commit: Failed to commit the transaction. " + e.getMessage());
                        continue;
                    }
                    catch (DataGridRuntimeException rtException)
                    {
                        System.err.println("Commit: Failed to commit the transaction. " + rtException.getMessage());
                        continue;
                    }
                    System.out.println("Commited the transaction.");
                }
                else if (input.equals("r"))
                {
                    // currently transactions are only supported for non-JDBC operations (e.g. put, get, delete)
                    if (!dgTxnSession)
                    {
                        System.out.println("Rollback is not supported for a non-transacted session ");
                        continue;
                    }
                    try
                    {
                        dgSession.rollback();
                    }
                    catch (DataGridException e)
                    {
                        System.out.println("Rollback: Failed to rollback the transaction. " + e.getMessage());
                        continue;
                    }
                    catch (DataGridRuntimeException rtException)
                    {
                        System.err.println("Rollback: Failed to rollback the transaction. " + rtException.getMessage());
                        continue;
                    }
                    System.out.println("Rolled back the transaction.");
                }
                else if (input.equals("ps"))
                {
                    // PreparedStatement processing
                    currentPreparedStatement = handlePreparedStatement(jdbcConnection, currentPreparedStatement);
                }
                else if (input.equals("s"))
                {
                    System.out.print("SQL statement: Enter the SQL statement (string): ");
                    String sqlString = readValue(br);
                    if (sqlString != null)
                    {
                        System.out.println("Executing statement: " + sqlString);
                        try (Statement statement = jdbcConnection.createStatement();)
                        {
                            if (statement.execute(sqlString))
                            {
                                try (ResultSet resultSet = statement.getResultSet();)
                                {
                                    printResultSet(resultSet);
                                }
                                catch (SQLException e)
                                {
                                    System.out.println("Failed to print ResultSet for: " + sqlString);
                                    printSQLException(e);
                                }
                            }
                            else
                            {
                                int rowCount = statement.getUpdateCount();
                                System.out.println("Successfully executed Statement: " + sqlString + ", rowCount: " + rowCount); 
                            }
                        }
                        catch (SQLException e)
                        {
                            System.out.println("Failed to execute statement: " + sqlString);
                            printSQLException(e);
                        }
                    }
                }
                else if (input.equals("u"))
                {
                    System.out.print("SQL Execute Update: Enter a sql DDL command (string): ");
                    String sqlString = readValue(br);
                    if (sqlString != null && !sqlString.equals(""))
                    {
                        System.out.println("Executing DDL command: " + sqlString);
                        try (Statement statement = jdbcConnection.createStatement())
                        {
                            int res = statement.executeUpdate(sqlString);
                            System.out.println("DDL command result: " + res);
                        }
                        catch (SQLException e)
                        {
                            System.out.println("Failed to execute DDL command: " + sqlString);
                            printSQLException(e);
                            continue;
                        }
                    }
                }
                else if (input.equals("md"))
                {
                    System.out.print("Metadata: Enter the metadata to display [g(rid), t(able)]: ");
                    String choice = readInput(br);
                    if (choice == null || choice.equals(""))
                    {
                        continue;
                    }
    
                    if (choice.equals("g") || choice.equals("grid"))
                    {
                        printGridMetadata(jdbcMetadata);
                    }
                    else if (choice.equals("t") || choice.equals("table"))
                    {
                        System.out.print("Metadata: Enter the name of the table to display (string): ");
                        String tname = readValue(br);
                        printTableMetadata(jdbcMetadata, tname);
                    }
                    else
                    {
                        System.out.println("Invalid metadata display option entered: " + choice);
                    }
                }
                else if (input.equals("h"))
                {
                    helpMenu(jdbcMetadata.supportsTransactions());
                }
                else if (!input.equals("q"))
                {
                    System.out.println("Unrecognized option: " + input);
                }
            }
            while (input == null || !input.equals("q"));
        }
        // re-throw any exceptions, we really just want a way to clean up
        // the objects we own in a finally block
        catch (DataGridException dgex)
        {
            throw dgex;
        }
        catch (DataGridRuntimeException rtex)
        {
            throw rtex;
        }
        catch (SQLException sqlex)
        {
            throw sqlex;
        }
        finally
        {
            // close open JDBC objects, Connection is closed in calling method
            closePStmt(currentPreparedStatement);

            // close all open AS data grid objects that we own
            closeRowArray();  // for 'pm' option
            
            closeAllTables();
            
            try
            {
                if (dgSession != null)
                {
                    System.out.println("Closing data grid session");
                    dgSession.close();
                }
            }
            catch (DataGridException dgex)
            {
                // ignore
            }
            catch (DataGridRuntimeException dgrtex)
            {
                // ignore
            }
            catch (Exception ex)
            {
                // ignore
            }
        }
    }
    
    private void helpMenu(boolean txnSupport)
    {
        String batchHelp = "";
        String txnHelp = "";
        if (txnSupport)
        {
            System.out.println("** This is a transacted session ***");
            
            txnHelp = "\n\tEnter 'c'  to commit a transaction"
                      + "\n\tEnter 'r'  to rollback a transaction";
        }
        else
        {
            batchHelp = "\n\tEnter 'pm' to put multiple rows into the table"
                        + "\n\tEnter 'gm' to get multiple rows from the table"
                        + "\n\tEnter 'dm' to delete multiple rows from the table";
        }

        System.out.println("\nASandJDBCClient commands:"
                           + "\n\tEnter 't'  to specify the table to use for non-JDBC operations"
                           + "\n\tEnter 'p'  to put a row into the table"
                           + "\n\tEnter 'g'  to get a row from the table"
                           + "\n\tEnter 'd'  to delete a row from the table"
                           + batchHelp
                           + "\n\tEnter 'i'  to iterate the rows in a table"
                           + txnHelp
                           + "\n\tEnter 's'  to create an SQL SELECT statement and execute it"
                           + "\n\tEnter 'u'  to execute an SQL DDL update"
                           + "\n\tEnter 'ps' to use a PreparedStatement to execute SQL commands"
                           + "\n\tEnter 'md' to display metadata about the grid and tables"
                           + "\n\tEnter 'h'  to display this help menu"
                           + "\n\tEnter 'q'  to quit");
    }
    
    private String readValue(BufferedReader br)
    {
        // reads the user input and returns it unmodified
        String input = null;
        try
        {
            input = br.readLine();
            if (input != null && input.isEmpty())
            {
                input = null;
            }
        }
        catch (IOException ioexception)
        {
            System.err.println("Error reading user input. " + ioexception.getMessage());
            input = null;
        }
        return input;
    }
    
    private String readInput(BufferedReader br)
    {
        // reads the user input and converts it to lower case
        String input = readValue(br);
        if (input != null)
        {
            input = input.toLowerCase();
        }

        return input;
    }
    
    private int readInteger(BufferedReader br, int defaultValue)
    {
        // reads the user input and converts it to an int
        String intStr = readValue(br);
        int intResult = 0;
        if (intStr == null)
        {
            intResult = defaultValue;
        }
        else
        {
            try
            {
                intResult = Integer.parseInt(intStr);
            }
            catch (NumberFormatException nfexception)
            {
                // return the default value
                intResult = defaultValue;
            }
        }
        return intResult;
    }
    
    private String readString(BufferedReader br, String defaultStr)
    {
        // reads the user input as a string and uses the default string
        // provided, if the user enters nothing
        String resultStr = readValue(br);
        if (resultStr == null || resultStr.isEmpty())
        {
            resultStr = defaultStr;
        }
        return resultStr;
    }
    
    /*** Table handling methods ***/
    
    // We allow multiple tables to be open at once so keep track of them
    // by name. Multiple open tables allows batching to work on them.
    private Map<String, Table> dgTableNameMap = new HashMap<String, Table>(4);
    
    // Keep track of which table operations should act upon
    private Table dgCurrentTable = null;
    private String dgCurrentTableName = null;
    
    private void initCurrentTable(Session session, Properties props, String tableName)
    {
        if (tableName == null)
        {
            return;
        }
        if (dgTableNameMap.containsKey(tableName))
        {
            dgCurrentTable = dgTableNameMap.get(tableName);
            dgCurrentTableName = tableName;
        }
        else
        {
            try
            { 
                dgCurrentTable = session.openTable(tableName, props);
                dgCurrentTableName = tableName;
                dgTableNameMap.put(dgCurrentTableName, dgCurrentTable);
            }
            catch (DataGridException dgex)
            {
                System.out.printf("Table: Error opening table '%s'. %s%n", tableName, dgex.getMessage());
                if (dgCurrentTableName != null)
                {
                    System.out.printf("Table: Current table '%s' is still in use.%n", dgCurrentTableName );
                }
            }
        }
        
    }
    
    private Table getCurrentTable()
    {
        return dgCurrentTable;
    }
    
    private String getCurrentTableName()
    {
        return dgCurrentTableName;
    }
    
    private void setCurrentTable(Session session, Properties props)
    {
        // The 't' option is used to specify the table to use for any further
        // non-SQL operations (e.g. put, get). Setting a new table will *not*
        // automatically close the last table that was set. This allows you
        // to have multiple tables open for batching operations. Tables
        // should be closed using the 'c' option whenever you are finished
        // using them.
        
        // if we exit without changing anything return the existing table
        String choice = "";
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
                
        System.out.print("Table: Enter the table option [s(et), c(lose)]: ");
        choice = readInput(br);
        if (choice != null && !choice.equals(""))
        {
            if (choice.equals("s") || choice.equals("set"))
            {
                System.out.print("Table: Enter the table name (string): ");
                String tname = readValue(br);
                if (tname != null)
                {
                    initCurrentTable(session, props, tname);
                }
                else
                {
                    System.out.println("Table: No table name entered.");
                    String currentTableName = getCurrentTableName();
                    if (currentTableName != null)
                    {
                        System.out.println("Table: Previous table '" + currentTableName + "' still active.");
                    }
                }
            }
            // test for cl to be entered since other sub-menus use cl for close
            else if (choice.equals("c") || choice.equals("cl") || choice.equals("close"))
            {
                String tableName = null;
                String currentTableName = getCurrentTableName();
                if (currentTableName != null && !currentTableName.isEmpty())
                {
                    System.out.printf("Table: Enter the table name (string) [%s]:", currentTableName);
                    tableName = readString(br, currentTableName);
                }
                else
                {
                    System.out.print("Table: Enter the table name (string): ");
                    tableName = readValue(br);
                }
                if (tableName != null && !tableName.equals(""))
                {
                    closeTable(tableName);
                }
            }
            else
            {
                System.out.println("Table: Invalid table option " + choice);
            }
        }
    }
    
    private void closeTable(String tableName)
    {
        if (tableName == null)
        {
            return;
        }
        
        Table table = dgTableNameMap.remove(tableName);
        if (table != null)
        {
            try
            {
                table.close();
                table = null;
            }
            catch (DataGridException dgex)
            {
                // ignore
            }
            System.out.printf("Table: Closed table '%s'.%n", tableName);
            if (dgCurrentTableName != null && dgCurrentTableName.equals(tableName))
            {
                dgCurrentTableName = null;
                dgCurrentTable = null;
                System.out.println("Table: No current table set.");
            }
        }
        else
        {
            System.out.printf("Table: Cannot close table '%s'. The table was not open.%n", tableName);
        }
    }
    
    private void closeAllTables()
    {
        if (!dgTableNameMap.isEmpty())
        {
            Iterator<Entry<String, Table>> it = dgTableNameMap.entrySet().iterator();
            while (it.hasNext())
            {
                Entry<String, Table> entry = it.next();
                String tableName = entry.getKey();
                Table table = entry.getValue();
                it.remove();  // remove the entry from the table name map
                if (table != null)
                {
                    try
                    {
                        table.close();
                        System.out.printf("Closed table: %s%n", tableName);
                    }
                    catch (DataGridException dgex)
                    {
                        // ignore
                    }
                }
            }
        }
    }
    
    private void populateColumn(Row putRow, String columnName, Integer sqlType, String columnValue) throws SQLDataException
    {
        if (columnValue == null)
        {
            return;
        }
        switch(sqlType)
        {
            case Types.BIGINT:
            {
                try
                {
                    Long value = Long.parseLong(columnValue);
                    putRow.setLong(columnName, value);
                }
                catch (NumberFormatException nfex)
                {
                    throw new SQLDataException("Cannot convert String value " + columnValue + " to a long");
                }
                catch (DataGridException dgex)
                {
                    throw new SQLDataException("Cannot store " + columnValue + " into column " + columnName + ". " + dgex.getMessage());
                }
                break;
            }
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
            {
                try
                {
                    Double value = Double.parseDouble(columnValue);
                    putRow.setDouble(columnName, value);
                }
                catch (NumberFormatException nfex)
                {
                    throw new SQLDataException("Cannot convert String value " + columnValue + " to a long");
                }
                catch (DataGridException dgex)
                {
                    throw new SQLDataException("Cannot store " + columnValue + " into column " + columnName + ". " + dgex.getMessage());
                }
                break;
            }
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            {
                try
                {
                    putRow.setString(columnName, columnValue);
                }
                catch (DataGridException dgex)
                {
                    throw new SQLDataException("Cannot store " + columnValue + " into column " + columnName + ". " + dgex.getMessage());
                }
                break;
            }
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
            {
                try
                {
                    byte[] barray = columnValue.getBytes(StandardCharsets.UTF_8);
                    putRow.setOpaque(columnName, barray);
                }
                catch (NumberFormatException nfex)
                {
                    throw new SQLDataException("Cannot convert String value " + columnValue + " to a byte array");
                }
                catch (DataGridException dgex)
                {
                    throw new SQLDataException("Cannot store " + columnValue + " into column " + columnName + ". " + dgex.getMessage());
                }
                break;
            }
            case Types.TIMESTAMP:
            {
                try
                {
                    // for TIMESTAMP only accept the number of milliseconds since the epoch
                    Long millis = Long.parseLong(columnValue);
                    TibDateTime dt = new TibDateTime();
                    dt.setFromMillis(millis);
                    putRow.setDateTime(columnName, dt);
                }
                catch (NumberFormatException nfex)
                {
                    throw new SQLDataException("Cannot convert String value " + columnValue + " to a long representing milliseconds");
                }
                catch (DataGridException dgex)
                {
                    throw new SQLDataException("Cannot store " + columnValue + " into column " + columnName + ". " + dgex.getMessage());
                }
                break;
            }
            default:
            {
                throw new SQLDataException("Unsupported SQL data type: " + sqlType);
            }
        }  // end switch
    }
    
    private Row populateRow(DatabaseMetaData dbMetadata)
    {
        String tableName = getCurrentTableName();
        Table table = getCurrentTable();
        if (tableName == null || table == null)
        {
            return null;
        }

        Row putRow = null;
        String errMsg = null;
        try
        {
            putRow = table.createRow();
            
            // use table meta data so column value prompts can be dynamic
            ResultSet rs = dbMetadata.getColumns(null, null, tableName, null);

            // dynamically prompt the user for each column of data to put in the row
            String userInput = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            while (rs.next())
            {
                // See the description of DatabaseMetaData.getColumns() to know
                // what the columns are for each row in the ResultSet
                String columnName = rs.getString("COLUMN_NAME");
                Integer sqlType = rs.getInt("DATA_TYPE");
                String sqlTypeName = rs.getString("TYPE_NAME");
                System.out.print("Enter the value for column '" + columnName + "' (" + sqlTypeName + "): ");
                userInput = readValue(br);
                if (userInput == null)
                {
                    continue;
                }
                populateColumn(putRow, columnName, sqlType, userInput);
            }
            rs.close();
        }
        catch (Exception ex)
        {
            errMsg = ex.getMessage();
        }
        
        if (errMsg != null)
        {
            System.err.println("Failed to populate row with data. " + errMsg);
            if (putRow != null)
            {
                try
                {
                    putRow.destroy();
                }
                catch (Exception ex)
                {
                    // ignore
                }
                putRow = null;
            }
        }
        return putRow;
    }
    
    private Row populatePrimaryKeyRow(DatabaseMetaData dbMetadata)
    {
        String tableName = getCurrentTableName();
        Table table = getCurrentTable();
        if (tableName == null || table == null)
        {
            return null;
        }

        Row keyRow = null;
        try
        {
            keyRow = table.createRow();
            
            // use table meta data so primary key column value prompts can be dynamic
            
            // DatabaseMetaData.getPrimaryKeys returns the columns ordered by column
            // name so we create a Map by column position as we prompt the user
            // to enter primary key column values in position order so what they
            // enter for their key column values will make sense to them.
            Map<Short, String> pkColumnPositionHash = new HashMap<Short, String>();
            ResultSet rs = dbMetadata.getPrimaryKeys(null, null, tableName);
            Short numPrimaryKeyColumns = 0;
            while (rs.next())
            {
                numPrimaryKeyColumns++;
                String columnName = rs.getString("COLUMN_NAME");
                Short columnPosition = rs.getShort("KEY_SEQ");
                pkColumnPositionHash.put(columnPosition, columnName);
            }
            rs.close();

            // dynamically prompt the user for primary key column values
            String userInput = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            for (Short i=1; i<=numPrimaryKeyColumns; i++)
            {
                String columnName = pkColumnPositionHash.get(i);
                rs = dbMetadata.getColumns(null, null, tableName, columnName);
                rs.next();
                if (!columnName.equals(rs.getString("COLUMN_NAME")))
                {
                    throw new Exception("Primary key column name mismatch for column: " + columnName);
                }
                Integer sqlType = rs.getInt("DATA_TYPE");
                String sqlTypeName = rs.getString("TYPE_NAME");
                rs.close();
                System.out.print("Enter the value for column '" + columnName + "' (" + sqlTypeName + "): ");
                userInput = readValue(br);
                if (userInput == null)
                {
                    continue;
                }
                populateColumn(keyRow, columnName, sqlType, userInput);
            }
        }
        catch (Exception ex)
        {
            System.out.println("Failed to populate primary key column(s) with data. " + ex.getMessage());
            
            if (keyRow != null)
            {
                try
                {
                    keyRow.destroy();
                }
                catch (Exception e)
                {
                    // ignore
                }
                keyRow = null;
            }
        }
        return keyRow;
    }
    
    /***** Put Multiple methods and data *****/

    private Row[] dgCurrentRowsToPut = null;  // array of rows to "put" using 'pm' option
    private int dgCurrentRowCount = 0;        // keep track of the number of rows in the array
    
    private void displayPutMultipleHelpMenu()
    {
        System.out.println("\nPut Multiple commands:"
                           + "\n\tEnter 'cr'   to create an array of rows to send"
                           + "\n\tEnter 'a'    to add rows to the current array of rows"
                           + "\n\tEnter 's'    to send the current array of rows in batches"
                           + "\n\tEnter 'cl'   to close the current array of rows without sending"
                           + "\n\tEnter 'q'    to exit this submenu");
        System.out.println("\nNote: Creating a new array of rows will close any existing array of rows.");
        System.out.println("      Sending the array of rows will close the array upon completion.");
    }
 
    private void handlePutMultiple(DatabaseMetaData jdbcMetadata, Session session)
    {
        displayPutMultipleHelpMenu();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String choice = "";

        do
        {
            System.out.print("\nPut Multiple: [ cr(eate)/a(dd)/s(end)/cl(ose)/q(uit) ]: ");
            choice = readInput(br);
            if (choice == null)
            {
                continue;
            }

            if (choice.equals("cr") || choice.equals("create"))
            {
                createRowArray();
            }
            else if (choice.equals("a") || choice.equals("add"))
            {
                if (dgCurrentRowsToPut == null)
                {
                    System.out.println("Put Multiple: No existing array of rows to add to.");
                    System.out.println("Use the 'cr' option to create an array of rows first.");
                    continue;
                }
                if (dgCurrentRowCount == dgCurrentRowsToPut.length)
                {
                    System.out.println("Put Multiple: No room left in array to add more rows.");
                    System.out.println("Use the 's' option to send the current array of rows.");
                    continue;
                }
                int previousRowCount = dgCurrentRowCount;
                addToRowArray(jdbcMetadata);
                
                int newRowsAdded = dgCurrentRowCount - previousRowCount;
                System.out.printf("Put Multiple: %d new rows added to current array of rows.%n", newRowsAdded);
            }
            else if (choice.equals("s") || choice.equals("send"))
            {
                if (dgCurrentRowsToPut == null || dgCurrentRowCount == 0)
                {
                    System.out.println("Put Multiple: No rows to send.");
                    System.out.println("Use the 'cr' option to create an array of rows.");
                    System.out.println("Then use the 'a' option to add rows to the array.");
                    continue;
                }
                
                // if rows from the array are sent, the row array will be cleared
                sendRowArray(session);
            }
            else if (choice.equals("cl") || choice.equals("close"))
            {
                if (dgCurrentRowsToPut == null)
                {
                    System.out.println("Put Multiple: No existing array of rows to close.");
                    System.out.println("Use the 'n' option to create an array of rows first.");
                    continue;
                }
                closeRowArray();
                System.out.println("Put Multiple: Closed current array of rows.");
            }
            else if (!choice.equals("q") && !choice.equals("quit"))       
            {
                System.out.println("Unknown option entered: " + choice);
            }
        }
        while (choice == null || (!choice.equals("q") && !choice.equals("quit")));

        if (dgCurrentRowCount > 0)
        {
            System.out.printf("Put Multiple: Exiting. Current rows waiting to be sent: %d%n", dgCurrentRowCount);
        }
    }
    
    private void createRowArray()
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        
        System.out.println("\nPut Multiple: Create a new array of rows to put.");
        System.out.printf("Enter the total number of rows to put (integer) [%d]: ", DEFAULT_OP_COUNT);
        int totalRows = readInteger(br, DEFAULT_OP_COUNT);
        if (totalRows <=0)
        {
            System.out.printf("Put Multiple: Invalid number of total rows entered: %d%n", totalRows);
            System.out.println("Enter a valid number greater than 0");
        }
        else
        {
            if (dgCurrentRowsToPut != null)
            {
                System.out.println("Put Multiple: Closing existing array of rows.");
                closeRowArray();
            }
            dgCurrentRowsToPut = new Row[totalRows];
            System.out.printf("Put Multiple: Created a new array %d of rows.%n", totalRows);
        }
    }
    
    private void closeRowArray()
    {
        if (dgCurrentRowsToPut != null)
        {
            for (Row row : dgCurrentRowsToPut)
            {
                if (row != null)
                {
                    try
                    {
                        row.destroy();
                    }
                    catch (DataGridException dgex)
                    {
                        // ignore
                    }
                }
            }
            dgCurrentRowsToPut = null;
            dgCurrentRowCount = 0;
        }
    }
    
    private void addToRowArray(DatabaseMetaData jdbcMetadata)
    {
        // Put Multiple will add rows into the array of rows where:
        //   1. the primary key value is incremented per row
        //   2. the specified string value has the primary key appended
        // For example: starting key = 0, string value = 'value'
        // Row 1 contents will be: key: 0, value: 'value 0'
        // Row 2 contents will be: key: 1, value: 'value 1'
        
        String tableName = getCurrentTableName();
        Table table = getCurrentTable();
        if (table == null || tableName == null)
        {
            return;
        }
        
        // check the number of primary key columns, put multiple will only
        // work when there is one primary key column for the table
        String pkColumnName = findPrimaryKeyColumnName(jdbcMetadata, tableName);       
        if (pkColumnName == null)
        {
            return;
        }
        
        int pkColumnSQLType = findPrimaryKeySQLType(jdbcMetadata, tableName, pkColumnName);
        if (pkColumnSQLType != Types.BIGINT && pkColumnSQLType != Types.VARCHAR)
        {
            // only support primary key columns of long or string
            System.out.println("Put Multiple: Unsupported primary key column type.");
            return;
        }
            
        // check the table for a non-primary key column which is of type VARCHAR
        // Put Mulitple will use the first VARCHAR column it encounters
        String varcharColumnName = findVarcharColumn(jdbcMetadata, tableName, pkColumnName, "%");
        if (varcharColumnName == null)
        {
            return;
        }
        
        // get the number of rows to add
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));        
        int numRows = readNumRowsToAdd(br);
        if (numRows == 0)
        {
            return;
        }
                
        System.out.print("Put Multiple: Enter the starting key (integer) [0]: ");
        int startKey = readInteger(br, 0);
                
        System.out.print("Put Multiple: Enter the value (string) [value]: ");
        String valueStr = readString(br, "value");
        
        System.out.printf("Put Multiple: Enter the value column name (string) [%s]: ", varcharColumnName);
        String valueColumnName = readString(br, varcharColumnName);
        if (!valueColumnName.equals(varcharColumnName))
        {
            varcharColumnName = findVarcharColumn(jdbcMetadata, tableName, pkColumnName, valueColumnName);
            if (varcharColumnName == null)
            {
                return;
            }
        }
        
        try
        {
            for (int i=0; i<numRows; i++)
            {
                // create a row in the next available slot of our row array
                dgCurrentRowsToPut[dgCurrentRowCount] = table.createRow();
                Row putRow = dgCurrentRowsToPut[dgCurrentRowCount];
                
                // populate the primary key column and the varchar column
                if (pkColumnSQLType == Types.BIGINT)
                {
                    putRow.setLong(pkColumnName, startKey);
                }
                else
                {
                    putRow.setString(pkColumnName, "" + startKey);
                }
                putRow.setString(varcharColumnName, valueStr + " " + startKey);
                dgCurrentRowCount++;
                startKey++;
            }
        }
        catch (DataGridException dgex)
        {
            System.out.println("Put Multiple: Failed to append row to array of rows.");
            System.out.println(dgex.getMessage());
        }
    }
    
    private String findPrimaryKeyColumnName(DatabaseMetaData jdbcMetadata, String tableName)
    {
        String pkColumnName = null;
        try
        {
            int numPkColumns = 0;
            ResultSet rs = jdbcMetadata.getPrimaryKeys(null, null, tableName);
            while (rs.next())
            {
                numPkColumns++;
                if (pkColumnName == null)
                {
                    pkColumnName = rs.getString("COLUMN_NAME");
                }
                else
                {
                    System.out.printf("Put Multiple: Table '%s' has > 1 column in its primary key.%n", tableName);
                    System.out.println("Put Multiple will only work with tables that have one primary key column.");
                    pkColumnName = null;
                    break;
                }
            }
            if (numPkColumns == 0)
            {
                System.out.printf("Put Multiple: Could not find primary key column for table '%s'.%n", tableName);
            }
        }
        catch (SQLException sqlex)
        {
            System.out.printf("Print Multiple: Error retrieving primary key column for table %s.%n", tableName);
            printSQLException(sqlex);
        }
        return pkColumnName;
    }
    
    private int findPrimaryKeySQLType(DatabaseMetaData jdbcMetadata, String tableName, String pkColumnName)
    {
        int sqlType = Types.OTHER;
        try
        {
            ResultSet rs = jdbcMetadata.getColumns(null, null, tableName, pkColumnName);
            while (rs.next())
            {
                sqlType = rs.getInt("DATA_TYPE");
                break;
            }
            if (sqlType == Types.OTHER)
            {
                System.out.printf("Put Multiple: Could not find SQL type of column '%s'.%n", pkColumnName);
            }
        }
        catch (SQLException sqlex)
        {
            System.out.printf("Print Multiple: Error retrieving SQL type of column '%s'.%n", pkColumnName);
            printSQLException(sqlex);
        }
        return sqlType;
    }
    
    private String findVarcharColumn(DatabaseMetaData jdbcMetadata, String tableName, String pkColumnName,
                                     String columnName)
    {
        String vcharColumnName = null;
        try
        {
            ResultSet rs = jdbcMetadata.getColumns(null, null, tableName, columnName);
            while (rs.next())
            {
                String columnNameStr = rs.getString("COLUMN_NAME");
                int sqlType = rs.getInt("DATA_TYPE");
                if (sqlType == Types.VARCHAR && !columnNameStr.equals(pkColumnName))
                {
                    vcharColumnName = columnNameStr;
                    break;
                }
            }
            if (vcharColumnName == null)
            {
                if (columnName == null || columnName.equals("%"))
                {
                    System.out.printf("Put Multiple: Could not find VARCHAR column for table '%s'.%n", tableName);
                }
                else
                {
                    System.out.printf("Put Multiple: Could not find non-primary key VARCHAR column '%s' in table '%s'.%n", columnName, tableName);
                }
            }
        }
        catch (SQLException sqlex)
        {
            System.out.printf("Print Multiple: Error retrieving VARCHAR column for table %s.%n", tableName);
            printSQLException(sqlex);
        }
        return vcharColumnName;
    }
    
    private int readNumRowsToAdd(BufferedReader br)
    {  
        int slotsAvailable = dgCurrentRowsToPut.length - dgCurrentRowCount;
        System.out.printf("%nPut Multiple: Enter the number of rows to add (integer)[%d]: " , slotsAvailable);
        int maxRows = readInteger(br, slotsAvailable);
        if (maxRows <=0 || maxRows > slotsAvailable)
        {
            System.out.printf("Put Multiple: Invalid number of rows entered: %d%n", maxRows);
            System.out.printf("Enter a valid number greater than 0 and less than %d%n", slotsAvailable);
            maxRows = 0;
        }
        return maxRows;
    }
    
    private void sendRowArray(Session session)
    {
        // check to ensure we have rows to send
        if (dgCurrentRowCount == 0)
        {
            return;
        }
        
        // get the number of rows per batch to send
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));        
        int batchSize = readBatchSize(br);
        if (batchSize == 0)
        {
            return;
        }
        
        int rowsSent = 0;
        int batchCount = 0;
        try
        {
            int rowsLeftToSend = dgCurrentRowCount;
            int currentRowNum = 0;
            do
            {
                List<Row> list = new ArrayList<Row>(batchSize);
                for (int i=0; i<batchSize; i++)
                {
                    list.add(dgCurrentRowsToPut[currentRowNum++]);
                    rowsLeftToSend--;
                    if (rowsLeftToSend == 0)
                    {
                        // we've reached the end of the array of rows
                        break;
                    }
                }
                
                int rowCount = list.size();
                if (rowCount > 0)
                {
                    try (BatchResult batch = session.putRows(list.toArray(new Row[rowCount]), null))
                    {
                        if (!batch.allSucceeded())
                        {
                            System.out.println("Put Multiple: Failed to send batch.");
                            break;
                        }
                        else
                        {
                            batchCount++;
                            rowsSent += rowCount;
                        }
                    }
                }
            }
            while (rowsLeftToSend > 0);
        }
        catch (DataGridException dgex)
        {
            System.out.println("Put Multiple: Failed to send array of rows.");
            System.out.println(dgex.getMessage());
        }
        
        System.out.printf("Put Multiple: %d rows sent (in %d batches).%n",
                          rowsSent, batchCount);

        // we don't keep track of where we are in the case of partial sends
        // so whether we were successful in sending or not, cleanup the array of rows
        // and the current row count
        System.out.println("Put Mulitple: closing array of rows");
        closeRowArray();
    }
    
    private int readBatchSize(BufferedReader br)
    {
        int maxSize = dgCurrentRowCount;
        int defaultSize = DEFAULT_BATCH_SIZE > maxSize ? maxSize : DEFAULT_BATCH_SIZE;
        System.out.printf("%nPut Multiple: Enter the number of put operations per batch (integer)[%d]: ", defaultSize);
        int batchSize = readInteger(br, defaultSize);
        if (batchSize <=0 || batchSize > maxSize)
        {
            System.out.printf("Put Multiple: Invalid batch size entered: %d%n", batchSize);
            System.out.printf("Enter a valid number greater than 0 and less than %d%n", maxSize);
            batchSize = 0;
        }
        return batchSize;
    }
    
    /*** get multiple methods ***/
    
    private void handleGetMultiple(DatabaseMetaData jdbcMetadata, Session session)
    {
        // get multiple supports getting rows in batches from the current table
        String tableName = getCurrentTableName();
        Table table = getCurrentTable();
        if (table == null || tableName == null)
        {
            return;
        }
        
        // check the number of primary key columns, get multiple will only
        // work when there is one primary key column for the table
        String pkColumnName = findPrimaryKeyColumnName(jdbcMetadata, tableName);       
        if (pkColumnName == null)
        {
            return;
        }
        
        int pkColumnSQLType = findPrimaryKeySQLType(jdbcMetadata, tableName, pkColumnName);
        if (pkColumnSQLType != Types.BIGINT && pkColumnSQLType != Types.VARCHAR)
        {
            // only support primary key columns of long or string
            System.out.println("Get Multiple: Unsupported primary key column type.");
            return;
        }
        
        // get the number of rows to get
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));        
        System.out.printf("%nGet Multiple: Enter the number of rows to get (integer)[%d]: ", DEFAULT_OP_COUNT);
        int numRows = readInteger(br, DEFAULT_OP_COUNT);
        if (numRows == 0)
        {
            System.out.println("Get Multiple: No rows retrieved.");
            return;
        }
        
        int defaultSize = DEFAULT_BATCH_SIZE > numRows ? numRows : DEFAULT_BATCH_SIZE;
        System.out.printf("%nGet Multiple: Enter the number of get operations per batch (integer)[%d]: ", defaultSize);
        int batchSize = readInteger(br, defaultSize);
        if (batchSize <=0 || batchSize > numRows)
        {
            System.out.printf("Get Multiple: Invalid batch size entered: %d%n", batchSize);
            System.out.printf("Enter a valid number greater than 0 and less than %d%n", numRows);
            return;
        }
                
        System.out.print("Get Multiple: Enter the starting key (integer) [0]: ");
        int startKey = readInteger(br, 0);
        System.out.println("Get Multiple: Using starting key of " + startKey);
        
        int getRowCount = 0;
        int batchCount = 0;
        int totalRowMissCount = 0;
        Row getRows[] = new Row[batchSize];
        try
        {
            // create an empty batch of rows
            for (int i=0; i<batchSize; i++)
            {
                getRows[i] = table.createRow();
            }
            
            int rowsLeftToGet = numRows;
            do
            {
                // use a list to hold the rows of the batch as the
                // batch might not be a fully populated array
                List<Row> list = new ArrayList<Row>(batchSize);
                for (int i=0; i<batchSize; i++)
                {
                    Row getRow = getRows[i];
                    
                    // populate the primary key column
                    if (pkColumnSQLType == Types.BIGINT)
                    {
                        getRow.setLong(pkColumnName, startKey);
                    }
                    else
                    {
                        getRow.setString(pkColumnName, "" + startKey);
                    }
                    list.add(getRow);
                    startKey++;
                    rowsLeftToGet--;
                    if (rowsLeftToGet == 0)
                    {
                        // we've reached the specified limit on the rows to get
                        break;
                    }
                }
                
                int rowCount = list.size();
                if (rowCount > 0)
                {
                    try (BatchResult batch = session.getRows(list.toArray(new Row[rowCount]), null))
                    {
                        if (!batch.allSucceeded())
                        {
                            System.out.printf("Get Multiple: Batch[%d] get failed.%n", batchCount+1);
                            break;
                        }
                        else
                        {
                            batchCount++;
                            getRowCount += rowCount;
                            
                            // the batch size will always be the same as the batch of keys we sent
                            // even if there was not a row in the table for the key
                            int size = batch.getSize();
                            int batchHitCount = 0;
                            int batchMissCount = 0;
                            for (int j=0; j<size; j++)
                            {
                                Row getRow = batch.getRow(j);
                                if (getRow != null)
                                {
                                    System.out.printf("Batch Row[%d]: %s%n", j+1, getRow);
                                    batchHitCount++;
                                }
                                else
                                {
                                    batchMissCount++;
                                }
                            }
                            System.out.printf("Get Multiple: Batch[%d]- Rows found: %d, Rows not found: %d%n",
                                              batchCount, batchHitCount, batchMissCount);
                            totalRowMissCount += batchMissCount;
                        }
                    }  // end try BatchResult
                }
            }
            while (rowsLeftToGet > 0);
        }
        catch (DataGridException dgex)
        {
            System.out.println("Get Multiple: Failed to get batch of rows. " + dgex.getMessage());
        }
        finally
        {
            for (Row getRow : getRows)
            {
                if (getRow != null)
                {
                    try
                    {
                        getRow.destroy();
                    }
                    catch (DataGridException e)
                    {
                        // ignore
                    }
                }
            }
        }
        System.out.printf("Get Multiple: Retrieved %d of %d rows (using %d batches) from table %s%n",
                          getRowCount - totalRowMissCount, getRowCount, batchCount, tableName);
    }
    
    /*** delete multiple methods ***/
    
    private void handleDeleteMultiple(DatabaseMetaData jdbcMetadata, Session session)
    {
        // delete multiple supports deleting rows in batches from the current table
        // this method is very similar to the handleGetMultiple method
        String tableName = getCurrentTableName();
        Table table = getCurrentTable();
        if (table == null || tableName == null)
        {
            return;
        }
        
        // check the number of primary key columns, delete multiple will only
        // work when there is one primary key column for the table
        String pkColumnName = findPrimaryKeyColumnName(jdbcMetadata, tableName);       
        if (pkColumnName == null)
        {
            return;
        }
        
        int pkColumnSQLType = findPrimaryKeySQLType(jdbcMetadata, tableName, pkColumnName);
        if (pkColumnSQLType != Types.BIGINT && pkColumnSQLType != Types.VARCHAR)
        {
            // only support primary key columns of long or string
            System.out.println("Delete Multiple: Only primary key columns of BIGINT or VARCHAR are supported.");
            return;
        }
        
        // get the number of rows to delete
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));        
        System.out.printf("%nDelete Multiple: Enter the number of rows to delete (integer)[%d]: ", DEFAULT_OP_COUNT);
        int numRows = readInteger(br, DEFAULT_OP_COUNT);
        if (numRows == 0)
        {
            System.out.println("Delete Multiple: No rows deleted.");
            return;
        }
        
        int defaultSize = DEFAULT_BATCH_SIZE > numRows ? numRows : DEFAULT_BATCH_SIZE;
        System.out.printf("%nDelete Multiple: Enter the number of delete operations per batch (integer)[%d]: ", defaultSize);
        int batchSize = readInteger(br, defaultSize);
        if (batchSize <=0 || batchSize > numRows)
        {
            System.out.printf("Delete Multiple: Invalid batch size entered: %d%n", batchSize);
            System.out.printf("Enter a valid number greater than 0 and less than %d%n", numRows);
            return;
        }
                
        System.out.print("Delete Multiple: Enter the starting key (integer) [0]: ");
        int startKey = readInteger(br, 0);
        System.out.println("Delete Multiple: Using starting key of " + startKey);
        
        int deleteRowCount = 0;
        int batchCount = 0;
        Row deleteRows[] = new Row[batchSize];
        try
        {
            // create an empty batch of rows
            for (int i=0; i<batchSize; i++)
            {
                deleteRows[i] = table.createRow();
            }
            
            int rowsLeftToDelete = numRows;
            do
            {
                // use a list to hold the rows of the batch as the
                // batch might not be a fully populated array
                List<Row> list = new ArrayList<Row>(batchSize);
                for (int i=0; i<batchSize; i++)
                {
                    Row deleteRow = deleteRows[i];
                    
                    // populate the primary key column
                    if (pkColumnSQLType == Types.BIGINT)
                    {
                        deleteRow.setLong(pkColumnName, startKey);
                    }
                    else
                    {
                        deleteRow.setString(pkColumnName, "" + startKey);
                    }
                    list.add(deleteRow);
                    startKey++;
                    rowsLeftToDelete--;
                    if (rowsLeftToDelete == 0)
                    {
                        // we've reached the specified limit on the rows to delete
                        break;
                    }
                }
                
                int rowCount = list.size();
                if (rowCount > 0)
                {
                    try (BatchResult batch = session.deleteRows(list.toArray(new Row[rowCount]), null))
                    {
                        if (!batch.allSucceeded())
                        {
                            System.out.printf("Delete Multiple: Batch[%d] delete failed.%n", batchCount+1);
                            break;
                        }
                        else
                        {
                            batchCount++;
                            int size = batch.getSize();
                            System.out.printf("Deleted %d rows in batch[%d] from table '%s'%n",
                                              size, batchCount, tableName);
                            deleteRowCount += rowCount;
                        }
                    }  // end try BatchResult
                }
            }
            while (rowsLeftToDelete > 0);
        }
        catch (DataGridException dgex)
        {
            System.out.println("Get Multiple: Failed to get batch of rows. " + dgex.getMessage());
        }
        finally
        {
            for (Row deleteRow : deleteRows)
            {
                if (deleteRow != null)
                {
                    try
                    {
                        deleteRow.destroy();
                    }
                    catch (DataGridException e)
                    {
                        // ignore
                    }
                }
            }
        }
        System.out.printf("Delete Multiple: Deleted %d rows (using %d batches) from table %s%n", deleteRowCount, batchCount, tableName);
    }
    
    /*** print methods ***/
    
    private void printGridMetadata(DatabaseMetaData jdbcMetadata) throws SQLException
    {
        if (jdbcMetadata == null)
        {
            return;
        }
        String productName = jdbcMetadata.getDatabaseProductName();
        System.out.printf("%s Version: %s.%s%n", productName,
                          jdbcMetadata.getDatabaseMajorVersion(), jdbcMetadata.getDatabaseMinorVersion());
        System.out.printf("%s Version: %s%n",
                          jdbcMetadata.getDriverName(), jdbcMetadata.getDriverVersion());
        
        ResultSet gridRs = jdbcMetadata.getCatalogs();
        if (gridRs.next())
        {
            System.out.println("Data Grid Name: " + gridRs.getString("TABLE_CAT"));
        }
        gridRs.close();

        int numTables = 0;
        ResultSet tableRs = jdbcMetadata.getTables(null, null, "%", null);
        if (!tableRs.isBeforeFirst() )
        {
            System.out.println("  No tables defined");
            tableRs.close();
            return;
        }

        while (tableRs.next())
        {
            numTables++;
            // get the third column which is the table name
            String tableName = tableRs.getString("TABLE_NAME");
            printTableMetadata(jdbcMetadata, tableName);
        }
        System.out.println("\nTotal Tables In Data Grid: " + numTables + "\n");
        tableRs.close();
    }
    
    // simple but not the most efficient way to convert a byte[] to a hex string
    public static String byteArrayToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(2 + (bytes.length * 2));
        sb.append("x'");
        for(byte b: bytes)
           sb.append(String.format("%02X", b));
        sb.append("'");
        return sb.toString();
     }
    
    private void printResultSet(ResultSet resultSet)
    {
        try
        {
            int rowCount = 0;
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnCount = rsmd.getColumnCount();
            while (resultSet.next())
            {
                if (rowCount == 0)
                {
                    // For functions in ResultSets, ResultSetMetaData might not contain the correct
                    // column type. So if this is a ResultSet from a query on the data grid 
                    // (versus a call on DatabaseMetaData which also returns a ResultSet),
                    // unwrap the ResultSet and display the column types from the first row of
                    // the ResultSet for any result column with a type of VARCHAR as functions
                    // default to returning VARCHAR.
                    //
                    // This is just to demonstrate how you can get access to the actual type of
                    // a column in a row of a ResultSet. Doing this should not be needed unless
                    // your ResultSet contains functions and ResultSetMetaData reports a type
                    // of VARCHAR instead of its real numeric type. NULL is reported as the
                    // type if a result column doesn't contain a value.
                    if (resultSet.isWrapperFor(com.tibco.datagrid.jdbc.ResultSetImplDG.class))
                    {
                        ResultSetImplDG rsImpl = resultSet.unwrap(com.tibco.datagrid.jdbc.ResultSetImplDG.class);
                        if (rsImpl != null)
                        {
                            System.out.printf("Result column types: ");
                            for (int c=1; c<=columnCount; c++)
                            {
                                if (c > 1)
                                {
                                    System.out.printf(", ");
                                }
                                String columnLabel = rsmd.getColumnLabel(c);
                                int columnType = rsmd.getColumnType(c);
                                if (columnType == Types.VARCHAR)
                                {
                                    int rsType = rsImpl.getColumnType(c);
                                    if (rsType != Types.NULL)
                                        columnType = rsType;
                                }
                                System.out.printf("%s: %s", columnLabel, JDBCType.valueOf(columnType).getName());
                            }
                            System.out.printf("%n");
                        }
                    }
                    System.out.println("ResultSet contents:");
                }
                System.out.printf("  row[%d]: ", ++rowCount);
                for (int i=1; i<=columnCount; i++)
                {
                    if (i > 1)
                    {
                        // format so each column is displayed separated by a comma and tab
                        System.out.printf(",\t ");
                    }
                    
                    String columnLabel = null;
                    String columnValue = null;
                    try
                    {
                        columnLabel = rsmd.getColumnLabel(i);
                        int sqlType = rsmd.getColumnType(i);
                        if (sqlType == Types.VARBINARY || sqlType == Types.BLOB
                                || sqlType == Types.BINARY || sqlType == Types.LONGVARBINARY)
                        {
                            byte[] barray = resultSet.getBytes(i);
                            if (barray != null)
                            {
                                if (outputFileName != null)
                                {
                                    try
                                    {
                                        String fname = outputFileName + "-" + outputFileCount++;
                                        DataOutputStream dos = new DataOutputStream(new FileOutputStream(fname));
                                        dos.write(barray, 0, barray.length);
                                        dos.close();
                                        columnValue = "binary data stored in file: " + fname;
                                    }
                                    catch (FileNotFoundException fnfex)
                                    {
                                        columnValue = null;
                                    }
                                    catch (IOException ioex)
                                    {
                                        columnValue = null;
                                    }
                                }
                                else
                                {
                                    // convert the byte[] into a hex string
                                    columnValue = byteArrayToHexString(barray);
                                }
                                if (columnValue == null)
                                {
                                    columnValue = "<binary>";
                                }
                            }
                            
                        }
                        else
                        {
                            columnValue = resultSet.getString(i);
                        }
                    }
                    catch (SQLException ex)
                    {
                        // do nothing
                    }
                    if (columnValue == null)
                    {
                        columnValue = "<NULL>";
                    }
                    System.out.printf("%s: %s", columnLabel, columnValue);
                }
                System.out.printf("%n");
            }
            System.out.printf("ResultSet contained %d rows%n", rowCount);
            rsmd = null;
        }
        catch (SQLException sqlex)
        {
            System.out.println("Failed to print ResultSet contents.");
            printSQLException(sqlex);
        } 
    }
    
    private void printTableMetadata(DatabaseMetaData jdbcMetadata, String tableName) throws SQLException
    {
        if (tableName == null)
        {
            System.out.println("printTableMetadata: Missing table name");
            return;
        }

        // check to ensure the table has been defined
        ResultSet tableRs = jdbcMetadata.getTables(null, null, tableName, null);
        if (!tableRs.isBeforeFirst() )
        {
            System.out.println("  Table " + tableName + " not found");
            tableRs.close();
            return;
        }
        tableRs.close();

        System.out.println("Table Name: " + tableName);
        ResultSet columnRs = jdbcMetadata.getColumns(null, null, tableName, null);
        while (columnRs.next())
        {
            // columns are ordered by column position
            Integer columnNumber = columnRs.getInt("ORDINAL_POSITION");
            String columnName = columnRs.getString("COLUMN_NAME");
            String columnType = columnRs.getString("TYPE_NAME");
            String columnNullable = columnRs.getString("IS_NULLABLE");    
            System.out.printf("  Column[%d] Name: %s\t Type: %s\t Nullable: %s%n", columnNumber, columnName, columnType, columnNullable);
        }
        columnRs.close();

        ResultSet pkRs = jdbcMetadata.getPrimaryKeys(null, null, tableName);
        String pkName = null;
        int pkColCount = 0;
        while (pkRs.next())
        {
            pkColCount++;
            if (pkColCount == 1)
            {
                pkName = pkRs.getString("PK_NAME");
                System.out.printf("  Primary Key Name: %s%n", pkName);
            }
            
            // primary key columns are ordered by column name
            String pkColumnName = pkRs.getString("COLUMN_NAME");
            Integer pkColumnSeq = pkRs.getInt("KEY_SEQ");
            System.out.printf("    Key Column[%d]: %s%n", pkColumnSeq, pkColumnName);
        }
        pkRs.close();

        ResultSet ixRs = jdbcMetadata.getIndexInfo(null, null, tableName, false, false);
        String previousIxName = null;
        while (ixRs.next())
        {
            String ixName = ixRs.getString("INDEX_NAME");
            if (ixName.equals(pkName))
            {
                // we've already displayed the primary key info
                continue;
            }
            
            if (previousIxName == null || !previousIxName.equals(ixName))
            {
                // index columns are ordered by index name
                // display the index name every time it changes
                System.out.printf("  Index Name: %s%n", ixName);
                previousIxName = ixName;
            }

            // index columns are listed by position then name
            String ixColumnName = ixRs.getString("COLUMN_NAME");
            Integer ixColumnSeq = ixRs.getInt("ORDINAL_POSITION");
            System.out.printf("    Index Column[%d]: %s%n", ixColumnSeq, ixColumnName);
        }
        ixRs.close();
    }
    
    /***** PreparedStatement methods *****/
    
    private void displayPStmtHelpMenu()
    {
        System.out.println("\nPreparedStatement commands:"
                           + "\n\tEnter 'cr'   to create a PreparedStatement"
                           + "\n\tEnter 'd'    to display ResultSetMetaData"
                           + "\n\tEnter 's'    to set values for parameters"
                           + "\n\tEnter 'e'    to execute the current PreparedStatement"
                           + "\n\tEnter 'cl'   to close the current PreparedStatement"
                           + "\n\tEnter 'q'    to exit this submenu");
        System.out.println("\nNote: Creating a PreparedStatement will close existing PreparedStatement");
    }
    
    private PreparedStatement handlePreparedStatement(Connection jdbcConnection,
                                                      PreparedStatement currentPreparedStatement)
    {
        PreparedStatement pstmt = currentPreparedStatement;
        displayPStmtHelpMenu();
        
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String choice = "";

        do
        {
            System.out.print("\nPreparedStatement: [ cr(eate)/d(isplay)/s(et)/e(xec)/cl(ose)/q(uit) ]: ");
            choice = readInput(br);
            if (choice == null)
            {
                continue;
            }
            
            if (choice.equals("cr") || choice.equals("create"))
            {
                System.out.print("\nPreparedStatement: Enter a SQL command (string): ");
                String sqlString = readValue(br);
                if (sqlString != null)
                {
                    closePStmt(pstmt);
                    pstmt = null;
                    
                    System.out.println("Creating PreparedStatement for: " + sqlString); 
                    try
                    {
                        pstmt = jdbcConnection.prepareStatement(sqlString);
                        System.out.println("Created PreparedStatement: " + sqlString);
                    }
                    catch (SQLException e)
                    {
                        System.out.println("Failed to create PreparedStatement: " + sqlString);
                        printSQLException(e);
                    }
                }
                else
                {
                    if (pstmt != null)
                    {
                        System.out.println("SQL string is empty. Previous PreparedStatement still in effect.");
                    }
                    else
                    {
                        System.out.println("SQL string is empty. No PreparedStatement created.");
                    }
                }
            }
            else if (choice.equals("d") || choice.equals("display"))
            {
                if (pstmt == null)
                {
                    System.out.println("No existing PreparedStatement to display ResultSetMetaData for.");
                    System.out.println("Use the 'cr' option to create a PreparedStatement first.");
                    continue;
                }
                try
                {
                    ResultSetMetaData rsmd = pstmt.getMetaData();
                    if (rsmd != null)
                        System.out.println("ResultSetMetadata: " + rsmd.toString());
                }
                catch (SQLException e)
                {
                    System.out.println("Failed to display ResultSetMetaData: " + e.toString());
                }
            }
            else if (choice.equals("s") || choice.equals("set"))
            {
                if (pstmt == null)
                {
                    System.out.println("No existing PreparedStatement to set parameter values for.");
                    System.out.println("Use the 'cr' option to create a PreparedStatement first.");
                    continue;
                }
                
                setParameters(pstmt);
            }
            else if (choice.equals("e") || choice.equals("exec"))
            {
                if (pstmt == null)
                {
                    System.out.println("No existing PreparedStatement to execute.");
                    System.out.println("Use the 'cr' option to create a PreparedStatement first.");
                    continue;
                }

                System.out.println("Executing PreparedStatement: " + pstmt.toString());
      
                try
                {
                    if (pstmt.execute())
                    {
                        try (ResultSet rs = pstmt.getResultSet();)
                        {
                            if (rs != null)
                            {
                                // a query was just executed, display the results
                                String action = null;
                                do
                                {
                                    System.out.println("Display ResultSet: [ a(ll)/n(ext)/c(olumn)/q(uit) ]");
                                    action = readInput(br);
                                    if (action == null)
                                    {
                                        continue;
                                    }
                                    
                                    if (action.equals("a") || action.equals("all"))
                                    {
                                        printResultSet(rs);
                                    }
                                    else if (action.equals("n") || action.equals("next"))
                                    {
                                        if (!rs.next())
                                        {
                                            System.out.println("Display ResultSet: End of results.");
                                        }
                                    }
                                    else if (action.equals("c") || action.equals("column"))
                                    {
                                        System.out.print("Enter column name: ");
                                        String columnName = readValue(br);
                                        if (columnName == null)
                                        {
                                            continue;
                                        }
                                        try
                                        {
                                            String svalue = rs.getString(columnName);
                                            System.out.println(columnName+": " + svalue);
                                        }
                                        catch (SQLException ex)
                                        {
                                            System.out.println("Could not retrieve column " + columnName + " as a string to display.");
                                            printSQLException(ex);
                                        }
                                    }
                                }
                                while (action == null || (!action.equals("a") && !action.equals("all") && !action.equals("q") && !action.equals("quit")));
                            }
                            else
                            {
                                int rowCount = pstmt.getUpdateCount();
                                System.out.println("Successfully executed PreparedStatment: " + pstmt.toString() + ", rowCount: " + rowCount); 
                            }
                        }
                        // try with resources ensures ResultSet.close() will get called
                        // let exception get caught in outer try/catch
                    }
                    else
                    {
                        System.out.println("Successfully executed PreparedStatement. Update count: " + pstmt.getUpdateCount());
                    }
                }
                catch (SQLException sqlex)
                {
                    System.out.println("Failed to execute PreparedStatement: " + pstmt.toString());
                    printSQLException(sqlex);
                } 
                catch (Exception ex)
                {
                    // shouldn't need this extra catch but DataGridRuntimeException is being thrown
                    // when it's not spec'd to be thrown
                    System.out.println("Failed to execute PreparedStatement: " + pstmt.toString());
                    System.out.println(ex.getMessage());
                }
            }
            else if (choice.equals("cl") || choice.equals("close"))
            {
                if (pstmt == null)
                {
                    System.out.println("No existing PreparedStatement to close.");
                    System.out.println("Use the 'cr' option to create a PreparedStatement first.");
                    continue;
                }
                closePStmt(pstmt);
                pstmt = null;
            }
            else if (!choice.equals("q") && !choice.equals("quit"))       
            {
                System.out.println("Unknown option entered: " + choice);
            }
        }
        while (choice == null || (!choice.equals("q") && !choice.equals("quit")));
        
        if (pstmt != null)
        {
            System.out.println("Current PreparedStatement: " + pstmt.toString());
        }
        
        return pstmt;
    }
    
    private void closePStmt(PreparedStatement pstmt)
    {
        if (pstmt != null)
        {
            System.out.println("Closing PreparedStatement: " + pstmt.toString());
            try
            {
                pstmt.close();  
            }
            catch (SQLException sqlex)
            {
                // ignore
            }
        }
    }
    
    private String readParameterTypeName(BufferedReader br)
    {
        String ptypeName = "";
        System.out.print("\nSetParameter: Parameters can be of type BIGINT, VARCHAR, DOUBLE, VARBINARY");
        System.out.print("\nSetParameter: Enter parameter type (string): ");
        ptypeName = readInput(br);
        if (ptypeName != null)
        {
            // validate the type entered
            if (!ptypeName.equals("bigint") && !ptypeName.equals("varchar")
                    && !ptypeName.equals("double") && !ptypeName.equals("varbinary"))
            {
                System.out.println("Invalid parameter type entered: " + ptypeName);
                ptypeName = null;
            }
        }
        return ptypeName;
    }
    
    private int getSQLType(String sqlTypeStr)
    {
        int sqlType = Types.OTHER;
        if (sqlTypeStr != null)
        {
            String upSQLTypeStr = sqlTypeStr.toUpperCase();
            if (upSQLTypeStr.equals("BIGINT"))
            {
                sqlType = Types.BIGINT;
            }
            else if (upSQLTypeStr.equals("VARCHAR"))
            {
                sqlType = Types.VARCHAR;
            }
            else if (upSQLTypeStr.equals("DOUBLE"))
            {
                sqlType = Types.DOUBLE;
            }
            else if (upSQLTypeStr.equals("VARBINARY"))
            {
                sqlType = Types.VARBINARY;
            }
        }
        return sqlType;
    }
    
    private void setBytesFromFile(PreparedStatement pstmt, Integer pnum, String fname) throws SQLException
    {
        if (fname == null)
        {
            throw new SQLException("missing file name");
        }
        
        File ifile = null;
        InputStream istream = null;
        try
        {
            ifile = new File(fname);
            istream = new FileInputStream(ifile);
            pstmt.setBinaryStream(pnum, istream);
        }
        catch (FileNotFoundException fnfex)
        {
            throw new SQLException("Invalid file name: " + fname);
        }
        finally
        {
            if (istream != null)
            {
                try
                {
                    istream.close();
                }
                catch (IOException ioex)
                {
                    System.out.printf("Failed to close FileInputStream for %s: %s%n", fname, ioex.getMessage());
                }
            }
        }
    }
    
    private byte[] hexStringToByteArray(String s) throws SQLException
    {
        String hexString = s;
        if (s.startsWith("x'"))
        {
            hexString = s.substring(1).replace("'","");
        }
        int len = hexString.length();
        if (len % 2 != 0)
        {
            throw new SQLException("Invalid hex string. Length must be evenly divisible by 2");
        }
        byte[] data = new byte[len / 2];
        for (int i=0; i<len; i+= 2)
        {
            data[i/2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                               + Character.digit(hexString.charAt(i+1), 16));
        }
        return data;
    }
    
    private void setParameters(PreparedStatement pstmt)
    {
        if (pstmt == null)
        {
            return;
        }
        
        // display the PreparedStatement so users can see the parameters and their positions
        System.out.println("PreparedStatement: " + pstmt.toString());

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String choice = "";
        do
        {
            System.out.print("\nSetParameter: Enter parameter number (int), q(uit) to exit: ");
            choice = readInput(br);
            if (choice == null)
            {
                continue;
            }
            
            if (!choice.equals("q") && !choice.equals("quit"))
            {
                Integer pnum = 0;
                try
                {
                    pnum = Integer.parseInt(choice);
                }
                catch (NumberFormatException nfex)
                {
                    // ignore pnum will be 0 and will be caught later as invalid
                }
                
                try
                {
                    String ptypeName = readParameterTypeName(br);
                    if (ptypeName == null)
                    {
                        continue;
                    }
                    String valueStr = "";
                    System.out.printf("%nSetParameter: Specify 'SQL NULL' for empty value.");
                    System.out.printf("%nSetParameter: Specify binary data as a hex string with an even number of characters inside the quotes. Format: x'ABCDEF'");
                    System.out.printf("%nSetParameter: Enter parameter value (%s): ", ptypeName);
                    valueStr = readValue(br);
                    if (valueStr == null)
                    {
                        continue;
                    }
                    
                    // the parameter SQL types are mapped to the default SQL types
                    // for the AS data types:
                    //    long => BIGINT
                    //    string => VARCHAR
                    //    double => DOUBLE
                    //    opaque => VARBINARY
                    //    datetime => TIMESTAMP
                    // we handle all types above except TIMESTAMP since TIMESTAMPs
                    // are compared as strings in WHERE clauses
                    int ptype = getSQLType(ptypeName);
                    if (valueStr.toLowerCase().equals("sql null"))
                    {
                        pstmt.setNull(pnum,  ptype);
                        continue;
                    }
                    switch(ptype)
                    {
                    case Types.BIGINT:
                    {
                        Long lvalue = Long.parseLong(valueStr);
                        pstmt.setLong(pnum, lvalue);
                        break;
                    }
                    case Types.VARCHAR:
                    {
                        pstmt.setString(pnum, valueStr);
                        break;
                    }
                    case Types.DOUBLE:
                    {
                        Double dvalue = Double.parseDouble(valueStr);
                        pstmt.setDouble(pnum, dvalue);
                        break;
                    }
                    case Types.VARBINARY:
                    {
                        String confirmStr = "";
                        System.out.printf("%nSetParameter: is this a file name [y/n]: ");
                        confirmStr = readString(br, "n");
                        if (confirmStr == null)
                        {
                            continue;
                        }
                        byte[] barray = null;
                        if (confirmStr.toLowerCase().equals("y"))
                        {
                            setBytesFromFile(pstmt, pnum, valueStr);
                        }
                        else
                        {
                            barray = hexStringToByteArray(valueStr);
                            pstmt.setBytes(pnum, barray);
                        }
                        break;
                    }
                    default:
                    {
                        System.out.printf("Cannot set parameter[%d] of type: %s%n", pnum, ptypeName);
                        System.out.println("Unhandled parameter type");
                        break;
                    }
                    }
                }
                catch (NumberFormatException nex)
                {
                    System.out.println("Failed to set parameter: " + pnum);
                    System.out.println(nex.toString());
                }
                catch (SQLException sqlex)
                {
                    System.out.println("Failed to set parameter: " + pnum);
                    printSQLException(sqlex);
                }
            }
        }
        while (choice == null || (!choice.equals("q") && !choice.equals("quit")));  
    }
}
