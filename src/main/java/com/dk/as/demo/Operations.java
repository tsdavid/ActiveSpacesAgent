/*
 * Copyright (c) 2016-2021 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 *
 * $Id: Operations.java 135749 2021-08-13 20:11:16Z $
 */

package com.dk.as.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import com.tibco.datagrid.BatchResult;
import com.tibco.datagrid.ColumnType;
import com.tibco.datagrid.Connection;
import com.tibco.datagrid.DataGrid;
import com.tibco.datagrid.DataGridException;
import com.tibco.datagrid.DataGridRuntimeException;
import com.tibco.datagrid.Event;
import com.tibco.datagrid.EventType;
import com.tibco.datagrid.GridMetadata;
import com.tibco.datagrid.ResultSet;
import com.tibco.datagrid.ResultSetMetadata;
import com.tibco.datagrid.Row;
import com.tibco.datagrid.RowSet;
import com.tibco.datagrid.Session;
import com.tibco.datagrid.Statement;
import com.tibco.datagrid.Table;
import com.tibco.datagrid.TableEventHandler;
import com.tibco.datagrid.TableListener;
import com.tibco.datagrid.TableMetadata;

/**
 * A sample program to illustrate the usage of the various APIs for accessing an ActiveSpaces Data Grid.
 *
 * The program assumes that a table has been configured that has a primary index that uses a column, called key,
 * of type long, and a second column, called value, of type string. By default the name of the table is t1, but
 * this can be changed by providing a different table name via the --tableName command line option.
 *
 * If the data grid is created by running the as-start (or as-start.bat) script that is provided with the installation
 * then the required table will be defined.
 */
public class Operations
{
    private static final String DEFAULT_GRIDURL = "http://localhost:8080";
    private static final String DEFAULT_GRID_NAME = null;
    private static final String DEFAULT_TABLE_NAME = "t1";
    private static final int DEFAULT_OP_COUNT = 10000;
    private static final int DEFAULT_BATCH_SIZE = 1024;

    private String url = DEFAULT_GRIDURL;
    private String gridName = DEFAULT_GRID_NAME;
    private String tableName = DEFAULT_TABLE_NAME;
    private String checkpointName = null;
    private double connectionTimeout = 20;
    private boolean doTxn = false;
    private String username = null;
    private String password = null;
    private String trustFileName = null;
    private boolean trustAll = false;
    private String logfile = null;
    private long logsize = 0;
    private int logcount = 0;
    Properties defaultProperties = null;

    private enum MetadataType {
        GRID,
        TABLE
    }

    private Operations() { }

    /**
     * Connect to the data grid, open the table then perform the operations requested by the user.
     *
     * @throws DataGridException if there was an error connecting to the data grid or opening the table
     */
    private void run() throws DataGridException
    {
        defaultProperties = createDefaultProperties();

        // Try-with-resources is being used, so a "finally" block isn't needed for
        // connection, initialGridMetadata, session, or table in this try block.
        try (Connection connection = DataGrid.connect(url, gridName, defaultProperties);
                GridMetadata initialGridMetadata = connection.getGridMetadata(defaultProperties))
        {
            validate(initialGridMetadata, tableName);

            try (Session session = connection.createSession(defaultProperties);
                    Table table = session.openTable(tableName, defaultProperties))
            {
                System.out.printf("%nConnected to table: %s%n", tableName);
                helpMenu(doTxn);
                userInputLoop(connection, session, doTxn, table, checkpointName);
            }
        }
    }

    /**********************************************************************************************
     *
     * Beginning of methods to demonstrate ActiveSpaces "per-op" functionality.
     *
     * Each method illustrates how to perform a different operation using the ActiveSpaces APIs.
     *
     **********************************************************************************************/

    /**
     * Insert a single row into the table.
     *
     * @param table the table to insert the row into
     * @param key the key for the row
     * @param value the value for the row
     */
    private void putRow(Table table, long key, String value)
    {
        // create a row and set the user supplied key and value in it
        Row putRow = null;
        try
        {
            putRow = table.createRow();
            putRow.setLong("key", key);
            if (value != null && !value.equals(""))
            {
                putRow.setString("value", value);
            }

            table.put(putRow);
            System.out.println("Put Success");

        }
        catch (DataGridException dataGridException)
        {
            dataGridException.printStackTrace(System.err);
        }
        finally
        {
            if (putRow != null)
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

    /**
     * Insert multiple rows into the table.
     *
     * Batches of rows are inserted into the table until the required number of rows have been inserted.
     *
     * @param session the session to use when doing the batch operation
     * @param table the table into which the rows are to be inserted
     * @param start the key of the first row
     * @param numRows the total number of rows to insert
     * @param batchSize the number of rows in each batch
     */
    private void putRows(Session session, Table table, long start, long numRows, int batchSize)
    {
        Row putRows[] = new Row[batchSize];
        long putCount = 0;
        long batchCount = 0;
        try
        {
            for (int i = 0; i < batchSize; i++)
            {
                putRows[i] = table.createRow();
            }

            long numBatches = 1 + (numRows - 1) / batchSize;
            long key = start;
            for (; batchCount < numBatches; batchCount++)
            {
                int rowsInThisBatch = Math.min(batchSize, Math.toIntExact((numRows + start) - key));

                Row[] array;
                if (rowsInThisBatch == batchSize)
                    array = putRows;
                else
                    array = Arrays.copyOf(putRows, rowsInThisBatch);

                int batchIndex = 0;
                for (; batchIndex < rowsInThisBatch; key++, batchIndex++)
                {
                    Row putRow = putRows[batchIndex];

                    putRow.setLong("key", key);
                    String value = "value " + key;
                    putRow.setString("value", value);
                }

                // Try-with-resources is being used, so a "finally" block isn't needed.
                try (BatchResult batch = session.putRows(array, defaultProperties))
                {
                    if (batch.allSucceeded())
                    {
                        putCount += rowsInThisBatch;
                    }
                    else
                    {
                        System.out.printf("Batch failed%n");
                        break;
                    }
                }

            }
        }
        catch (DataGridException dataGridException)
        {
            dataGridException.printStackTrace(System.err);
        }
        finally
        {
            for (Row putRow : putRows)
            {
                if (putRow != null)
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
        System.out.printf("Put %d rows (%d batches) in the table %n", putCount, batchCount);
    }

    /**
     * Get a single row from the table. The value of the row is printed, if it exists.
     *
     * @param table the table to get the row from
     * @param key the key for the row
     */
    private void getRow(Table table, long key)
    {
        // create a row and set the user supplied key and value in it
        Row keyRow = null;
        try
        {
            keyRow = table.createRow();
            keyRow.setLong("key", key);
            Row getRow = table.get(keyRow);
            if (getRow != null)
            {
                System.out.printf("Row retrieved from table: %s%n", getRow.toString());
                getRow.destroy();
            }
            else
            {
                System.out.println("Row does not exist");
            }
        }
        catch (DataGridException dataGridException)
        {
            dataGridException.printStackTrace(System.err);
        }
        finally
        {
            if (keyRow != null)
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

    /**
     * Get multiple rows from the table.
     *
     * Batches of rows are requested from the table until the required number of rows have been retrieved.
     *
     * @param session the session to use when doing the batch operation
     * @param table the table from which the rows are to be requested
     * @param start the key of the first row
     * @param numRows the total number of rows to get
     * @param batchSize the number of rows in each batch
     */
    private void getRows(Session session, Table table, long start, long numRows, int batchSize)
    {
        Row keyRows[] = new Row[batchSize];
        long getCount = 0;
        long batchCount = 0;
        try
        {
            for (int i = 0; i < batchSize; i++)
            {
                keyRows[i] = table.createRow();
            }

            long numBatches = 1 + (numRows - 1) / batchSize;
            long key = start;
            for (; batchCount < numBatches; batchCount++)
            {
                int rowsInThisBatch = Math.min(batchSize, Math.toIntExact((numRows + start) - key));

                Row[] array;
                if (rowsInThisBatch == batchSize)
                    array = keyRows;
                else
                    array = Arrays.copyOf(keyRows, rowsInThisBatch);

                int batchIndex = 0;
                for (; batchIndex < rowsInThisBatch; key++, batchIndex++)
                {
                    Row keyRow = keyRows[batchIndex];

                    keyRow.setLong("key", key);
                }

                try (BatchResult batch = session.getRows(array, defaultProperties))
                {
                    if (batch.allSucceeded())
                    {
                        getCount += rowsInThisBatch;
                        int size = batch.getSize();
                        System.out.printf("Batch contains %d results%n", size);
                        for (int j = 0; j < size; j++)
                        {
                            Row getRow = batch.getRow(j);
                            if (getRow != null)
                            {
                                System.out.printf("Row retrieved from table: %s%n", getRow);
                            }
                            else
                            {
                                System.out.println("Row does not exist");
                            }
                        }
                    }
                    else
                    {
                        System.out.printf("Batch failed%n");
                        break;
                    }
                }
            }
        }
        catch (DataGridException dataGridException)
        {
            dataGridException.printStackTrace(System.err);
        }
        finally
        {
            for (Row keyRow : keyRows)
            {
                if (keyRow != null)
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

        System.out.printf("Got %d rows (%d batches) in the table %n", getCount, batchCount);
    }

     /**
      * Update a single row in the table. It is not necessary to update every column in the row
      *
      * @param table the table to insert the row into
      * @param key the key for the row
      * @param value the value to update in the row
      */
     private void updateRow(Table table, long key, String value)
     {
         // create a row and set the user supplied key and value in it
         Row updateRow = null;
         try
         {
             updateRow = table.createRow();
             updateRow.setLong("key", key);
             if (value != null && !value.equals(""))
             {
                 updateRow.setString("value", value);
             }

             long rowsModified = table.update(updateRow);
             System.out.println("Update Success: " + rowsModified + " rows modified");
         }
         catch (DataGridException dataGridException)
         {
             dataGridException.printStackTrace(System.err);
         }
         finally
         {
             if (updateRow != null)
             {
                 try
                 {
                     updateRow.destroy();
                 }
                 catch (DataGridException e)
                 {
                     e.printStackTrace(System.err);
                 }
             }
         }
     }

    /**
     * Delete a single row from the table.
     *
     * @param table the table to delete the row from
     * @param key the key for the row
     */
    private void deleteRow(Table table, long key)
    {
        // create a row and set the user supplied key and value in it
        Row keyRow = null;
        try
        {
            keyRow = table.createRow();
            keyRow.setLong("key", key);
            table.delete(keyRow);
            System.out.println("Delete Success");
        }
        catch (DataGridException dataGridException)
        {
            dataGridException.printStackTrace(System.err);
        }
        finally
        {
            if (keyRow != null)
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

    /**
     * Delete multiple rows from the table.
     *
     * Batches of rows are deleted from the table until the required number of rows have been deleted.
     *
     * @param session the session to use when doing the batch operation
     * @param table the table from which the rows are to be deleted
     * @param start the key of the first row
     * @param numRows the total number of rows to delete
     * @param batchSize the number of rows in each batch
     */
    private void deleteRows(Session session, Table table, long start, long numRows, int batchSize)
    {
        Row keyRows[] = new Row[batchSize];
        long deleteCount = 0;
        long batchCount = 0;
        try
        {
            for (int i = 0; i < batchSize; i++)
            {
                keyRows[i] = table.createRow();
            }

            long numBatches = 1 + (numRows - 1) / batchSize;
            long key = start;
            for (; batchCount < numBatches; batchCount++)
            {
                int rowsInThisBatch = Math.min(batchSize, Math.toIntExact((numRows + start) - key));

                Row[] array;
                if (rowsInThisBatch == batchSize)
                    array = keyRows;
                else
                    array = Arrays.copyOf(keyRows, rowsInThisBatch);

                int batchIndex = 0;
                for (; batchIndex < rowsInThisBatch; key++, batchIndex++)
                {
                    Row deleteRow = keyRows[batchIndex];

                    deleteRow.setLong("key", key);
                }

                try (BatchResult batch = session.deleteRows(array, defaultProperties))
                {
                    if (batch.allSucceeded())
                    {
                        deleteCount += rowsInThisBatch;
                    }
                    else
                    {
                        System.out.printf("Batch failed%n");
                        break;
                    }
                }
            }
        }
        catch (DataGridException dataGridException)
        {
            dataGridException.printStackTrace(System.err);
        }
        finally
        {
            for (Row keyRow : keyRows)
            {
                if (keyRow != null)
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

        System.out.printf("Deleted %d rows (%d batches) in the table %n", deleteCount, batchCount);
    }

    /**
     * Iterate over the rows in the table.
     *
     * @param table the table to iterate over
     * @param filter if this is not null only rows that match this filter will be received
     */
    private void iterateTable(Table table, String filter)
    {
        System.out.printf("\tUsing filter: %s%n", filter);

        // Try-with-resources is being used, so a "finally" block isn't needed.
        try (RowSet rowSet = table.createRowSet(filter, defaultProperties))
        {
            long rowCount = 0;
            try
            {
                for (Row itrRow : rowSet)
                {
                    rowCount++;
                    try
                    {
                        System.out.printf("Row from iterator: %s%n", itrRow.toString());
                    }
                    finally
                    {
                        try
                        {
                            itrRow.destroy();
                        }
                        catch (DataGridException e)
                        {
                            e.printStackTrace(System.err);
                        }
                    }
                }
            }
            catch (NoSuchElementException ex)
            {
                System.out.println("No such element exception received during iteration.");
                ex.printStackTrace(System.err);
            }

            System.out.printf("Iterated over %d rows in the table %n", rowCount);
        }
        catch (DataGridException e)
        {
            e.printStackTrace(System.err);
        }
    }

    /**
     * Create a listener on the table and listen for events.
     *
     * @param table the table to listen to
     * @param filter if this is not null only events that match this filter will be received
     */
    private void listenToTableEvents(Table table, String filter, String eventTypes)
    {
        System.out.printf("\tUsing filter: %s%n", filter);

        class ListenerState
        {
            long count = 0;
            TableListener listener = null;
            ReentrantLock lock = new ReentrantLock();
            boolean closed = false;
            Properties props = null;
        }
        final ListenerState listenerState = new ListenerState();

        Properties props = new Properties(defaultProperties);
        if (eventTypes != null && !eventTypes.isEmpty())
        {
            props.setProperty(TableListener.TIBDG_LISTENER_PROPERTY_STRING_EVENT_TYPE_LIST, eventTypes);
        }
        listenerState.props = props;

        try
        {
            listenerState.listener = table.createTableListener(filter, new TableEventHandler()
            {
                @Override
                public void eventsReceived(List<Event> events, TableListener listener)
                {
                    try
                    {
                        for (Event event : events)
                        {
                            // Reference listenerState object defined outside the callback
                            listenerState.count++;

                            EventType eventType = event.getType();
                            System.out.printf("Received event %d of type %s on table %s:%n", listenerState.count, eventType, table.getName());
                            Row previous = null;
                            Row current = null;
                            boolean errorEvent = false;
                            switch (eventType)
                            {
                                case PUT:
                                    previous = event.getPrevious();
                                    current = event.getCurrent();
                                    break;
                                case DELETE:
                                case EXPIRED:
                                    previous = event.getPrevious();
                                    break;
                                case ERROR:
                                    errorEvent = true;
                                    break;
                                default:
                                    break;
                            }
                            if (previous != null)
                            {
                                System.out.printf("\tprevious: %s%n", previous.toString());
                            }
                            if (current != null)
                            {
                                System.out.printf("\tcurrent: %s%n", current.toString());
                            }
                            if (errorEvent)
                            {
                                System.out.printf("errorcode: %d description: %s%n", event.getErrorCode(),
                                        event.getErrorDescription());
                            }

                            if(eventType == EventType.ERROR)
                            {
                                int errorCode = event.getErrorCode();
                                if(errorCode == Event.TIBDG_EVENT_ERRORCODE_PROXY_FAILURE ||
                                        errorCode == Event.TIBDG_EVENT_ERRORCODE_COPYSET_LEADER_FAILURE ||
                                        errorCode == Event.TIBDG_EVENT_ERRORCODE_GRID_REDISTRIBUTING)
                                {
                                    listenerState.lock.lock();
                                    try
                                    {
                                        if(!listenerState.closed)
                                        {
                                            Table listenerTable = listener.getTable();
                                            String listenerFilter = listener.getFilter();
                                            listener.close();
                                            listener = null;
                                            listenerState.listener = null;
                                            System.out.println("Recreating listener after error...");

                                            try
                                            {
                                                listenerState.listener = listenerTable.createTableListener(listenerFilter, this, props);
                                            }
                                            catch (Exception e)
                                            {
                                                e.printStackTrace(System.err);
                                            }

                                            int delay = 1;
                                            while (listenerState.listener == null)
                                            {
                                                System.out.println("Failed to recreate listener. Retrying again in " + delay + " seconds");
                                                Thread.sleep(delay * 1000);
                                                try
                                                {
                                                    listenerState.listener = listenerTable.createTableListener(listenerFilter, this, props);
                                                }
                                                catch (Exception e)
                                                {
                                                    e.printStackTrace(System.err);
                                                }
                                            }
                                            System.out.println("Listening on table...");
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        e.printStackTrace(System.err);
                                    }
                                    finally
                                    {
                                        listenerState.lock.unlock();
                                    }
                                }
                                else
                                {
                                    System.out.println("Listener now invalid.  Will not receive more events.");
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace(System.err);
                    }
                }
            }, listenerState.props);

            System.out.println("Table Listener listening on table. Press [ENTER] to stop listening");
            try
            {
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII));
                br.readLine();
            }
            catch (IOException ioexception)
            {
                ioexception.printStackTrace(System.err);
            }
            System.out.println("Destroying Table Listener.");
        }
        catch (DataGridException e)
        {
            e.printStackTrace(System.err);
        }
        finally
        {

            listenerState.lock.lock();
            try
            {
                listenerState.closed = true;
                if(listenerState.listener != null)
                {
                    listenerState.listener.close();
                }
                listenerState.listener = null;
            }
            catch (DataGridException e)
            {
                e.printStackTrace(System.err);
            }
            finally
            {
                listenerState.lock.unlock();
            }
        }

    }

    /**
     * Create a SQL statement and execute it.
     *
     * @param session the session on which to create the statement
     * @param sqlString the SQL that will be executed
     */
    private void executeSqlStatement(Session session, String sqlString)
    {
        System.out.printf("Creating statement: %s%n", sqlString);

        // Statement extends Java 7 auto closeable
        // Try-with-resources is also supported to avoid needing the "finally" block.
        Statement statement = null;
        ResultSet resultSet = null;

        try
        {
            statement = session.createStatement(sqlString, defaultProperties);
            ResultSetMetadata rsm = statement.getResultSetMetadata();
            if (rsm != null)
            {
                // process this as a SELECT statement
                resultSet = statement.executeQuery(defaultProperties);

                int columnCount = rsm.getColumnCount();
                System.out.printf("Result set contains %d columns:%n", columnCount);
                // column numbers start with 1
                for (int i = 1; i <= columnCount; i++)
                {
                    String columnName = rsm.getColumnName(i);
                    ColumnType columnType = rsm.getColumnType(i);
                    System.out.printf("\t%s\t%s%n", columnName, columnType.toString());
                }

                long rowCount = 0;
                try
                {
                    for (Row row : resultSet)
                    {
                        rowCount++;
                        try
                        {
                            System.out.printf("Row from result set: %s%n", row.toString());
                        }
                        finally
                        {
                            try
                            {
                                row.destroy();
                            }
                            catch (DataGridException e)
                            {
                                e.printStackTrace(System.err);
                            }
                        }
                    }
                }
                catch (NoSuchElementException ex)
                {
                    System.out.println("No such element exception received during iteration.");
                    ex.printStackTrace(System.err);
                }
                System.out.printf("ResultSet contained %d rows.%n", rowCount);
            }
            else
            {
                long rowCount = statement.executeUpdate(defaultProperties);
                System.out.printf("Successfully executed statement. %d rows updated.%n", rowCount);
            }
        }
        catch (DataGridException e)
        {
            System.out.println("Failed to execute the statement.");
            e.printStackTrace(System.err);
        }
        catch (Exception ex)
        {
            // unspecified exception caught
            System.out.println("Failed to execute the statement.");
            ex.printStackTrace(System.err);
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch (Exception e)
                {
                    System.out.println("Failed to close ResultSet.");
                    e.printStackTrace(System.err);
                }
            }
            if (statement != null)
            {
                try
                {
                    statement.close();
                }
                catch (Exception e)
                {
                    System.out.println("Failed to close Statement.");
                    e.printStackTrace(System.err);
                }
            }
        }
    }

    /**
     * Execute a SQL DDL statement.
     *
     * @param session the session to use when executing the statement
     * @param sqlString the SQL that will be executed
     */
    private void executeSqlUpdate(Session session, String sqlString)
    {
        try
        {
            if (sqlString != null && !sqlString.equals(""))
            {
                System.out.printf("Executing update: %s%n", sqlString);
            }
            else
            {
                sqlString = null;
            }

            long res = session.executeUpdate(sqlString, defaultProperties);
            System.out.printf("Result of executing the update: %d%n", res);
        }
        catch (DataGridException e)
        {
            System.out.println("Failed to execute the update.");
            e.printStackTrace(System.err);
        }
    }

    /**
     * Get the metadata for either the whole data grid or just the table, and print it.
     *
     * @param connection the connection to use when retrieving the metadata
     * @param mdType the type of metadata to print
     * @param tableName the name of the table
     * @param checkpointName the name of the checkpoint, if set
     */
    private void getMetadata(Connection connection, MetadataType mdType, String tableName, String checkpointName)
    {
        Properties props = defaultProperties;
        if (checkpointName != null)
        {
            props = new Properties(defaultProperties);
            props.setProperty(GridMetadata.TIBDG_GRIDMETADATA_PROPERTY_STRING_CHECKPOINT_NAME, checkpointName);
        }

        // Try-with-resources is being used, so a "finally" block isn't needed.
        try (GridMetadata gridMetadata = connection.getGridMetadata(props))
        {
            switch (mdType)
            {
                case GRID:
                    printGridMetadata(gridMetadata);
                    break;
                case TABLE:
                    TableMetadata tableMetadata = gridMetadata.getTableMetadata(tableName);
                    if (tableMetadata == null)
                    {
                        System.err.printf("Table '%s' not found%n", tableName);
                    }
                    else
                    {
                        printTableMetadata(tableMetadata);
                    }
                    break;
                default:
                    System.out.println("Invalid metadata display option entered");
                    break;
            }
        }
        catch (DataGridException dataGridException)
        {
            dataGridException.printStackTrace(System.err);
        }
    }

    /**
     * Print the metadata for the specific table.
     *
     * @param tableMetadata the metadata for the table
     * @throws DataGridException if there was an error accessing the table's metadata
     */
    private void printTableMetadata(TableMetadata tableMetadata) throws DataGridException
    {
        System.out.printf("Table '%s':%n", tableMetadata.getName());
        System.out.printf("\tName = %s%n", tableMetadata.getName());
        if (tableMetadata.getDefaultTTL() != 0)
            System.out.printf("\tExpiration enabled, default TTL = %d%n", tableMetadata.getDefaultTTL());
        System.out.printf("\tPrimary Index = %s%n", tableMetadata.getPrimaryIndexName());

        String[] columnNames = tableMetadata.getColumnNames();

        System.out.printf("\t%d columns%n", columnNames.length);
        for (String columnName : columnNames)
        {
            System.out.printf("\t\t%s (%s)%n", columnName, tableMetadata.getColumnType(columnName));
        }

        String[] indexNames = tableMetadata.getIndexNames();

        System.out.printf("\t%d indexes%n", indexNames.length);
        for (String indexName : indexNames)
        {
            System.out.printf("\t\t%s (", indexName);

            String[] indexColumnNames = tableMetadata.getIndexColumnNames(indexName);
            System.out.printf(String.join(", ", indexColumnNames));
            System.out.printf(")%n");
        }
        System.out.printf("%n");
    }

    /**
     * Print the metadata for the whole data grid.
     *
     * @param gridMetadata the metadata for the whole data grid
     * @throws DataGridException if there was an error accessing the data grid's metadata
     */
    private void printGridMetadata(GridMetadata gridMetadata) throws DataGridException
    {
        System.out.printf("AS Product Version: '%s'%nGrid Name: '%s'%n",
                gridMetadata.getVersion(), gridMetadata.getGridName());

        String[] tableNames = gridMetadata.getTableNames();

        System.out.printf("The data grid contains %d table(s):%n", tableNames.length);

        for (String tableName : tableNames)
        {
            TableMetadata tableMetadata = gridMetadata.getTableMetadata(tableName);
            printTableMetadata(tableMetadata);
        }
    }

    /**********************************************************************************************
     *
     * End of methods to demonstrate ActiveSpaces "per-op" functionality.
     *
     **********************************************************************************************/

    /**
     * Create a default Properties object to be used for all operations.
     *
     * The Properties object controls aspects of the Connection and Session such as how long to wait for a connection to
     * be established, any username, password and trust information, and whether the session is transacted or not.
     * 
     * Other properties may be inherited from System properties.
     *
     * @return a Properties object
     */
    private Properties createDefaultProperties()
    {
        Properties props = new Properties(System.getProperties());
        props.setProperty(Connection.TIBDG_CONNECTION_PROPERTY_STRING_CLIENT_LABEL, "operations Java sample");
        props.setProperty(Connection.TIBDG_CONNECTION_PROPERTY_DOUBLE_TIMEOUT, String.valueOf(connectionTimeout));
        props.setProperty(Session.TIBDG_SESSION_PROPERTY_BOOLEAN_TRANSACTED, String.valueOf(doTxn));

        if (username != null && password != null)
        {
            props.setProperty(Connection.TIBDG_CONNECTION_PROPERTY_STRING_USERNAME, username);
            props.setProperty(Connection.TIBDG_CONNECTION_PROPERTY_STRING_USERPASSWORD, password);
        }

        boolean secureRealm = url.toLowerCase().startsWith("https://");

        if (secureRealm)
        {
            if (trustAll)
            {
                props.setProperty(Connection.TIBDG_CONNECTION_PROPERTY_STRING_TRUST_TYPE,
                        Connection.TIBDG_CONNECTION_HTTPS_CONNECTION_TRUST_EVERYONE);
            }
            else if (trustFileName != null)
            {
                props.setProperty(Connection.TIBDG_CONNECTION_PROPERTY_STRING_TRUST_TYPE,
                        Connection.TIBDG_CONNECTION_HTTPS_CONNECTION_USE_SPECIFIED_TRUST_FILE);
                props.setProperty(Connection.TIBDG_CONNECTION_PROPERTY_STRING_TRUST_FILE,
                        trustFileName);
            }
        }

        if (checkpointName != null)
        {
            props.setProperty(Session.TIBDG_SESSION_PROPERTY_STRING_CHECKPOINT_NAME,
                    checkpointName);
        }

        return props;
    }

    /**
     * Set the log level.
     *
     * @param logArg one of "debug", "info", "off", "severe", "verbose", "warn"
     * @throws Exception if there was an error setting the log level
     */
    private void setLogLevel(String logArg) throws Exception
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
            case "api":
                logLevel = "tibdgapi:debug3";
                break;

            default:
                logLevel = logArg;
        }

        DataGrid.setLogLevel(logLevel);
    }

    /**
     * Checks that a table with the given name exists in the data grid and that it has the appropriate columns
     *
     * @param gridMetadata the metadata for the whole data grid
     * @param tableName the name of the table
     * @throws DataGridException if there was an error accessing the table and its schema
     */
    private void validate(GridMetadata gridMetadata, String tableName) throws DataGridException
    {
        // Display the product version info before validating
        System.out.printf("AS Product Version: %s%n", gridMetadata.getVersion());

        // check that the table to be used exists
        TableMetadata tableMetadata = gridMetadata.getTableMetadata(tableName);
        if (tableMetadata == null)
        {
            System.err.printf("No table '%s' configured in the data grid%n", tableName);
            throw new RuntimeException("Table not found");
        }

        // check that the user specified table has the appropriate columns
        // this example only supports tables with a primary key column
        // of type long and named 'key' and a string column named 'value'
        ColumnType keyType = tableMetadata.getColumnType("key");
        if (keyType != ColumnType.LONG)
        {
            System.err.printf("Column 'key' is not of type Long");
            throw new RuntimeException("Column 'key' is not of type Long");
        }

        ColumnType valueType = tableMetadata.getColumnType("value");
        if (valueType != ColumnType.STRING)
        {
            System.err.printf("Column 'value' is not of type String");
            throw new RuntimeException("Column 'value' is not of type String");
        }
    }

    /**
     * Collect input from the user and call the appropriate method to perform the requested operation.
     *
     * @param connection the connection to the data grid
     * @param session the session to use for the operations
     * @param table the table to use for the operations
     * @param checkpointName the name of the checkpoint, if set
     */
    private void userInputLoop(Connection connection, Session session, boolean doTxn, Table table, String checkpointName)
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII));
        String input = "";

        do
        {
            if (doTxn)
            {
                System.out.println("** This is a transacted session **");
                System.out.print("Main: [p/g/d/up/i/c/r/l/s/u/md/h/q]: ");
            }
            else
            {
                System.out.print("Main: [p/g/d/up/pm/gm/dm/i/l/s/u/md/h/q]: ");
            }

            try
            {
                input = br.readLine();
            }
            catch (IOException ioexception)
            {
                ioexception.printStackTrace(System.err);
            }

            if (input == null)
                continue;

            try
            {
                switch (input)
                {
                    case "p":
                    {
                        long key;
                        String value = "";
                        try
                        {
                            System.out.print("Put: Enter the key (long): ");
                            input = br.readLine();
                            try
                            {
                                key = Long.parseLong(input);
                            }
                            catch (NumberFormatException nfexception)
                            {
                                System.out.println("Please enter a long value");
                                continue;
                            }
                            System.out.print("Put: Enter the value (string): ");
                            value = br.readLine();

                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }

                        putRow(table, key, value);
                        break;
                    }
                    case "pm":
                    {
                        long numRows, start;
                        int batchSize;

                        System.out.printf("Put Multiple: Enter the number of rows to put (long)[%d]: ", DEFAULT_OP_COUNT);
                        try
                        {
                            input = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }
                        try
                        {
                            if (input == null)
                                continue;

                            if (input.isEmpty())
                            {
                                numRows = DEFAULT_OP_COUNT;
                            }
                            else
                            {
                                numRows = Long.parseLong(input);
                            }
                        }
                        catch (NumberFormatException nfexception)
                        {
                            System.out.println("Please enter a long value");
                            continue;
                        }

                        if (numRows <= 0)
                        {
                            System.out.println("The number of rows must be greater than 0");
                            continue;
                        }

                        System.out.printf("Put Multiple: Enter the number of put operations per batch (integer)[%d]: ",
                                DEFAULT_BATCH_SIZE);
                        try
                        {
                            input = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }
                        try
                        {
                            if (input == null)
                                continue;

                            if (input.isEmpty())
                            {
                                batchSize = DEFAULT_BATCH_SIZE;
                            }
                            else
                            {
                                batchSize = Integer.parseInt(input);
                            }
                        }
                        catch (NumberFormatException nfexception)
                        {
                            System.out.println("Please enter an integer value");
                            continue;
                        }

                        if (batchSize <= 0)
                        {
                            System.out.println("The number of rows per batch must be greater than 0");
                            continue;
                        }

                        System.out.print("Put Multiple: Enter the starting key number (long)[0]: ");
                        try
                        {
                            input = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }
                        try
                        {
                            if (input == null)
                                continue;

                            if (input.isEmpty())
                            {
                                start = 0;
                            }
                            else
                            {
                                start = Long.parseLong(input);
                            }
                        }
                        catch (NumberFormatException nfexception)
                        {
                            System.out.println("Please enter a long value");
                            continue;
                        }

                        putRows(session, table, start, numRows, batchSize);
                        break;
                    }
                    case "g":
                    {
                        long key;
                        try
                        {
                            System.out.print("Get: Enter the key (long): ");
                            input = br.readLine();
                            try
                            {
                                key = Long.parseLong(input);
                            }
                            catch (NumberFormatException nfexception)
                            {
                                System.out.println("Please enter a long value");
                                continue;
                            }

                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }

                        getRow(table, key);
                        break;
                    }
                    case "gm":
                    {
                        long numRows, start;
                        int batchSize;

                        System.out.printf("Get Multiple: Enter the number of rows to get (long)[%d]: ", DEFAULT_OP_COUNT);
                        try
                        {
                            input = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }
                        try
                        {
                            if (input == null)
                                continue;

                            if (input.isEmpty())
                            {
                                numRows = DEFAULT_OP_COUNT;
                            }
                            else
                            {
                                numRows = Long.parseLong(input);
                            }
                        }
                        catch (NumberFormatException nfexception)
                        {
                            System.out.println("Please enter a long value");
                            continue;
                        }

                        if (numRows <= 0)
                        {
                            System.out.println("The number of rows must be greater than 0");
                            continue;
                        }

                        System.out.printf("Get Multiple: Enter the number of get operations per batch (integer)[%d]: ",
                                DEFAULT_BATCH_SIZE);
                        try
                        {
                            input = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }
                        try
                        {
                            if (input == null)
                                continue;

                            if (input.isEmpty())
                            {
                                batchSize = DEFAULT_BATCH_SIZE;
                            }
                            else
                            {
                                batchSize = Integer.parseInt(input);
                            }
                        }
                        catch (NumberFormatException nfexception)
                        {
                            System.out.println("Please enter an integer value");
                            continue;
                        }

                        if (batchSize <= 0)
                        {
                            System.out.println("The number of rows per batch must be greater than 0");
                            continue;
                        }

                        System.out.print("Get Multiple: Enter the starting key number (long)[0]: ");
                        try
                        {
                            input = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }
                        try
                        {
                            if (input == null)
                                continue;

                            if (input.isEmpty())
                            {
                                start = 0;
                            }
                            else
                            {
                                start = Long.parseLong(input);
                            }
                        }
                        catch (NumberFormatException nfexception)
                        {
                            System.out.println("Please enter a long value");
                            continue;
                        }

                        getRows(session, table, start, numRows, batchSize);
                        break;
                    }
                    case "d":
                    {
                        long key;
                        try
                        {
                            System.out.print("Delete: Enter the key (long): ");
                            input = br.readLine();
                            try
                            {
                                key = Long.parseLong(input);
                            }
                            catch (NumberFormatException nfexception)
                            {
                                System.out.println("Please enter a long value");
                                continue;
                            }

                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }

                        deleteRow(table, key);
                        break;
                    }
                    case "dm":
                    {
                        long numRows, start;
                        int batchSize;

                        System.out.printf("Delete Multiple: Enter the number of rows to delete (long)[%d]: ", DEFAULT_OP_COUNT);
                        try
                        {
                            input = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }
                        try
                        {
                            if (input == null)
                                continue;

                            if (input.isEmpty())
                            {
                                numRows = DEFAULT_OP_COUNT;
                            }
                            else
                            {
                                numRows = Long.parseLong(input);
                            }
                        }
                        catch (NumberFormatException nfexception)
                        {
                            System.out.println("Please enter a long value");
                            continue;
                        }

                        if (numRows <= 0)
                        {
                            System.out.println("The number of rows must be greater than 0");
                            continue;
                        }

                        System.out.printf("Delete Multiple: Enter the number of delete operations per batch (integer)[%d]: ",
                                DEFAULT_BATCH_SIZE);
                        try
                        {
                            input = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }
                        try
                        {
                            if (input == null)
                                continue;

                            if (input.isEmpty())
                            {
                                batchSize = DEFAULT_BATCH_SIZE;
                            }
                            else
                            {
                                batchSize = Integer.parseInt(input);
                            }
                        }
                        catch (NumberFormatException nfexception)
                        {
                            System.out.println("Please enter an integer value");
                            continue;
                        }

                        if (batchSize <= 0)
                        {
                            System.out.println("The number of rows per batch must be greater than 0");
                            continue;
                        }

                        System.out.print("Delete Multiple: Enter the starting key number (long)[0]: ");
                        try
                        {
                            input = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }
                        try
                        {
                            if (input == null)
                                continue;

                            if (input.isEmpty())
                            {
                                start = 0;
                            }
                            else
                            {
                                start = Long.parseLong(input);
                            }
                        }
                        catch (NumberFormatException nfexception)
                        {
                            System.out.println("Please enter a long value");
                            continue;
                        }

                        deleteRows(session, table, start, numRows, batchSize);
                        break;
                    }
                    case "up":
                    {
                        long key;
                        String value = "";
                        try
                        {
                            System.out.print("Update: Enter the key (long): ");
                            input = br.readLine();
                            try
                            {
                                key = Long.parseLong(input);
                            }
                            catch (NumberFormatException nfexception)
                            {
                                System.out.println("Please enter a long value");
                                continue;
                            }
                            System.out.print("Update: Enter the value (string): ");
                            value = br.readLine();

                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }

                        updateRow(table, key, value);
                        break;
                    }
                    case "i":
                    {
                        String filter = null;
                        try
                        {
                            System.out.print("Iterate: Enter the filter (string): ");
                            filter = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }

                        if (filter != null && filter.isEmpty())
                            filter = null;

                        iterateTable(table, filter);
                        break;
                    }
                    case "l":
                    {
                        String filter = null;
                        String eventTypes = null;
                        try
                        {
                            System.out.print("Listen: Enter the filter (string): ");
                            filter = br.readLine();
                            System.out.print("Enter comma separated event types (put,delete,expired) to listen to or omit for all events: ");
                            eventTypes = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }

                        if (filter != null && filter.isEmpty())
                            filter = null;

                        listenToTableEvents(table, filter, eventTypes);
                        break;
                    }
                    case "c":
                    {
                        if (!doTxn)
                        {
                            System.out.println("Commit is not supported for a non transacted session ");
                            continue;
                        }

                        try
                        {
                            session.commit();
                        }
                        catch (DataGridException e)
                        {
                            System.out.println("Failed to commit the transaction.");
                            e.printStackTrace(System.err);
                            continue;
                        }
                        System.out.println("Commited the transaction.");
                        break;
                    }
                    case "r":
                    {
                        if (!doTxn)
                        {
                            System.out.println("Rollback is not supported for a non transacted session ");
                            continue;
                        }
                        try
                        {
                            session.rollback();
                        }
                        catch (DataGridException e)
                        {
                            System.out.println("Failed to rollback the transaction.");
                            e.printStackTrace(System.err);
                            continue;
                        }
                        System.out.println("Rolled back the transaction.");
                        break;
                    }
                    case "s":
                    {
                        String sqlString;
                        try
                        {
                            System.out.print("SQL Statement: Enter the sql string (string): ");
                            sqlString = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }

                        if (sqlString == null || sqlString.isEmpty())
                        {
                            continue;
                        }

                        executeSqlStatement(session, sqlString);
                        break;
                    }
                    case "u":
                    {
                        String sqlString;
                        try
                        {
                            System.out.print("SQL Execute Update: Enter the sql string (string): ");
                            sqlString = br.readLine();
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }

                        executeSqlUpdate(session, sqlString);
                        break;
                    }
                    case "md":
                        try
                        {
                            System.out.print("Metadata: Enter the metadata to display [g(rid), t(able)]: ");
                            input = br.readLine();
                            if (input == null || input.equals(""))
                            {
                                continue;
                            }
                        }
                        catch (IOException ioexception)
                        {
                            ioexception.printStackTrace(System.err);
                            continue;
                        }

                        MetadataType mdType;
                        switch (input)
                        {
                            case "g":
                                mdType = MetadataType.GRID;
                                break;
                            case "t":
                                mdType = MetadataType.TABLE;
                                break;
                            default:
                                System.out.println("Invalid metadata display option entered");
                                continue;
                        }

                        String tableName;
                        try
                        {
                            tableName = table.getName();
                        }
                        catch (DataGridException e)
                        {
                            e.printStackTrace(System.err);
                            continue;
                        }
                        getMetadata(connection, mdType, tableName, checkpointName);
                        break;

                    case "q":
                        break;

                    case "h":
                    default:
                        helpMenu(doTxn);
                        break;
                }
            }
            catch (DataGridRuntimeException dataGridRuntimeException)
            {
                System.out.println("Received runtime exception while executing request: " + dataGridRuntimeException.toString());
                dataGridRuntimeException.printStackTrace(System.err);
            }
        }
        while (input != null && !input.equals("q"));

    }

    /**
     * Prints the help menu that lists the available operations.
     */
    private void helpMenu(boolean doTxn)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(System.lineSeparator());
        builder.append("Operations commands:").append(System.lineSeparator());
        builder.append("\tEnter 'p'  to put a row into the table").append(System.lineSeparator());
        builder.append("\tEnter 'g'  to get a row from the table").append(System.lineSeparator());
        builder.append("\tEnter 'd'  to delete a row from the table").append(System.lineSeparator());
        builder.append("\tEnter 'up'  to update a row in the table").append(System.lineSeparator());
        if (!doTxn)
        {
            builder.append("\tEnter 'pm' to put multiple rows into the table").append(System.lineSeparator());
            builder.append("\tEnter 'gm' to get multiple rows from the table").append(System.lineSeparator());
            builder.append("\tEnter 'dm' to delete multiple rows from the table").append(System.lineSeparator());
        }
        builder.append("\tEnter 'i'  to iterate the rows in a table").append(System.lineSeparator());
        if (doTxn)
        {
            builder.append("\tEnter 'c'  to commit a transaction").append(System.lineSeparator());
            builder.append("\tEnter 'r'  to rollback a transaction").append(System.lineSeparator());
        }
        builder.append("\tEnter 'l'  to listen to changes to the table").append(System.lineSeparator());
        builder.append("\tEnter 's'  to create an SQL SELECT or DML statement and execute it").append(System.lineSeparator());
        builder.append("\tEnter 'u'  to execute an SQL DDL update").append(System.lineSeparator());
        builder.append("\tEnter 'md' to display metadata about the data grid and tables").append(System.lineSeparator());
        builder.append("\tEnter 'h'  to display this help menu").append(System.lineSeparator());
        builder.append("\tEnter 'q'  to quit").append(System.lineSeparator());

        System.out.print(builder.toString());
    }

    /**
     * Prints the command line options.
     */
    private void usage()
    {
        System.out.printf("Usage: java [-Dcom.tibco.tibdg.<property>=<value> ...] %s [options] %n",
                this.getClass().getCanonicalName());
        System.out.printf("Options:%n");
        System.out.printf("\t-h or --help      display command line options.%n");
        System.out.printf("\t-r                Url of the realm server (Default %s)%n", DEFAULT_GRIDURL);
        System.out.printf("\t-g or --gridName  name of the grid (Default %s)%n", DEFAULT_GRID_NAME);
        System.out.printf("\t--tableName       name of the table (Default %s)%n", DEFAULT_TABLE_NAME);
        System.out.printf("\t--timeout         Connection timeout value (Default 20 sec)%n");
        System.out.printf("\t--txn             Create a transacted session. (Default false)%n");
        System.out.printf("\t--checkpoint      name of the checkpoint (Default null)%n");
        System.out.printf("\t--username        username%n");
        System.out.printf("\t--password        password%n");
        System.out.printf("\t--trust-file      Realm Server PEM trust file, incompatible with -trust-all%n");
        System.out.printf("\t--trust-all       Do not verify identity of realm servers%n");
        System.out.printf("\t--logLevel        debug, info, off, severe, verbose, warn (Default info)%n");
        System.out.printf("\t--logfile         prefix of log file to use instead of stdout (Default none)%n");
        System.out.printf("\t--logsize         maximum size of log files (Default 1mb)%n");
        System.out.printf("\t--logcount        how many log files to rotate between (Default 2)%n");
    }

    /**
     * Parses the command line arguments.
     *
     * @param args the command line arguments
     */
    private void parseArgs(String[] args) throws IllegalArgumentException
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
            else if (flag.equalsIgnoreCase("-r") && i < argc - 1)
            {
                url = args[++i];
            }
            else if (flag.equalsIgnoreCase("-timeout") && i < argc - 1)
            {
                try
                {
                    connectionTimeout = Double.valueOf(args[++i]);
                }
                catch (NumberFormatException nfex)
                {
                    String errMsg = "Invalid value for command line argument '" + args[i-1] + "', value: '" + args[i] + "'";
                    throw new IllegalArgumentException(errMsg);
                }
            }
            else if (flag.equalsIgnoreCase("-txn") && i < argc - 1)
            {
                doTxn = Boolean.valueOf(args[++i]);
            }
            else if ((flag.equalsIgnoreCase("-g") || flag.equalsIgnoreCase("-gridName")) && i < argc - 1)
            {
                gridName = args[++i];
            }
            else if (flag.equalsIgnoreCase("-tableName") && i < argc - 1)
            {
                tableName = args[++i];
            }
            else if (flag.equalsIgnoreCase("-checkpoint") && i < argc - 1)
            {
                checkpointName = args[++i];
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
            else if (flag.equalsIgnoreCase("-logLevel") && i < argc - 1)
            {
                try
                {
                    setLogLevel(args[++i]);
                }
                catch (Exception ex)
                {
                    String errMsg = "Failed setting log level";
                    throw new IllegalArgumentException(errMsg);
                }
            }
            else if (flag.equalsIgnoreCase("-logfile") && i < argc - 1)
            {
                logfile = args[++i];
            }
            else if (flag.equalsIgnoreCase("-logsize") && i < argc - 1)
            {
                try
                {
                    logsize = Long.parseLong(args[++i]);
                }
                catch (Exception ex)
                {
                    String errMsg = "Failed to set log size '" + args[i] + ", reason: " + ex.getMessage();
                    throw new IllegalArgumentException(errMsg);
                }
            }
            else if (flag.equalsIgnoreCase("-logcount") && i < argc - 1)
            {
                try
                {
                    logcount = Integer.parseInt(args[++i]);
                }
                catch (Exception ex)
                {
                    String errMsg = "Failed to set log count '" + args[i] + ", reason: " + ex.getMessage();
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
        if (logfile != null)
        {
            try
            {
                long filesize = (logsize != 0) ? logsize : 2000000;
                int  maxfiles = (logcount != 0) ? logcount : 50;
                DataGrid.setLogFiles(logfile, filesize, maxfiles, null);
            }
            catch (Exception ex)
            {
                String errMsg = "Failed setting log files";
                throw new IllegalArgumentException(errMsg);
            }
        }
    }

    public static void main(String[] args)
    {
        System.out.println("ActiveSpaces Example: Operations");
        try
        {
            Operations operations = new Operations();
            boolean runOperations = true;
            try
            {
                operations.parseArgs(args);
            }
            catch (IllegalArgumentException pex)
            {
                // there was an error parsing the command-line arguments
                if (pex.getMessage() != null)
                    System.out.println(pex.getMessage());
                operations.usage();
                runOperations = false;
            }
            if (runOperations)
            {
                operations.run();
            }
        }
        catch (DataGridException dataGridException)
        {
            dataGridException.printStackTrace(System.err);
        }
    }
}
