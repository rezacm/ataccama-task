package com.ataccama.homework.service;

import com.ataccama.homework.model.Database;
import com.ataccama.homework.repository.DatabaseRepository;
import com.ataccama.homework.rest.exception.DBConnectException;
import com.ataccama.homework.rest.exception.DataProcessingException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The service for retrieveing information from the database. Data processing is implemented using java.sql package.
 * More information about the parameters (as well as arguments during the ResultSet parsing) that are used in getColumns,
 * getSchemas and getTables method can be found here https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html
 */
@Service
public class DatabaseService {

    private final DatabaseRepository databaseRepository;

    @Autowired
    public DatabaseService(DatabaseRepository databaseRepository) {
        this.databaseRepository = databaseRepository;
    }

    /**
     * Method returns data from the database table.
     *
     * @param dbInstanceName name of the database instance.
     * @param schemaName     name of the schema.
     * @param tableName      name of the table in the schema.
     * @return JSONArray object that stores data from the table.
     * @throws DBConnectException      in case of database connection problem.
     * @throws DataProcessingException in case of problems during the data processing.
     */
    public JSONArray getTableData(String dbInstanceName, String schemaName, String tableName) throws DBConnectException, DataProcessingException {
        JSONArray jsonArray = new JSONArray();

        try (Connection connection = connectionFactory(dbInstanceName)) {

            String query = "SELECT * FROM " + schemaName + "." + tableName;

            ResultSet resultSet = connection
                    .createStatement()
                    .executeQuery(query);

            JSONArray columns = listColumns(dbInstanceName, schemaName, tableName);


            while (resultSet.next()) {
                JSONObject record = new JSONObject();

                for (int i = 0; i < columns.length(); i++) {
                    String columnName = columns.getJSONObject(i).getString("nsame");
                    record.put(columnName, resultSet.getObject(columnName));
                }

                jsonArray.put(record);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataProcessingException();
        }

        return jsonArray;
    }


    /**
     * Method returns the list of schemas in database.
     *
     * @param dbInstanceName name of the database instance.
     * @return JSONArray object that contains the list of schemas in the database.
     * @throws DBConnectException      in case of database connection problem.
     * @throws DataProcessingException in case of problems during the data processing.
     */
    public JSONArray listSchemas(String dbInstanceName) throws DBConnectException, DataProcessingException {
        JSONArray jsonArray = new JSONArray();

        try (Connection connection = connectionFactory(dbInstanceName)) {

            ResultSet schemas = connection
                    .getMetaData()
                    .getSchemas();


            while (schemas.next()) {
                JSONObject record = new JSONObject();

                record.put("name", schemas.getString(1));
                record.put("catalog", schemas.getString(2));

                jsonArray.put(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataProcessingException();
        }

        return jsonArray;
    }

    /**
     * Method returns the list of tables in the database schema.
     *
     * @param dbInstanceName name of the database instance.
     * @param schemaName     name of the schema.
     * @return JSONArray object that contains the list of the table in the schema.
     * @throws DBConnectException      in case of database connection problem.
     * @throws DataProcessingException in case of problems during the data processing.
     */
    public JSONArray listTables(String dbInstanceName, String schemaName) throws DBConnectException, DataProcessingException {
        JSONArray jsonArray = new JSONArray();

        try (Connection connection = connectionFactory(dbInstanceName)) {

            ResultSet tables = connection
                    .getMetaData()
                    .getTables(null, schemaName, "%", new String[]{"TABLE"});


            while (tables.next()) {
                JSONObject record = new JSONObject();

                record.put("catalog", tables.getString(1));
                record.put("schema", tables.getString(2));
                record.put("name", tables.getString(3));

                jsonArray.put(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataProcessingException();
        }

        return jsonArray;
    }

    /**
     * Method returns list of columns in the database table.
     *
     * @param dbInstanceName name of the database instance.
     * @param schemaName     name of the schema.
     * @param tableName      name of the table in the schema.
     * @return JSONArray object that contains the list of the columns
     * @throws DBConnectException      in case of database connection problem.
     * @throws DataProcessingException in case of problems during the data processing.
     */
    public JSONArray listColumns(String dbInstanceName, String schemaName, String tableName) throws DBConnectException, DataProcessingException {
        JSONArray jsonArray = new JSONArray();

        try (Connection connection = connectionFactory(dbInstanceName)) {

            List pks = listPKs(dbInstanceName, schemaName, tableName);


            ResultSet columns = connection
                    .getMetaData()
                    .getColumns(null, schemaName, tableName, null);


            while (columns.next()) {
                JSONObject column = new JSONObject();

                column.put("name", columns.getString(4));
                column.put("type", columns.getString(5));
                column.put("size", columns.getString(7));
                column.put("nullable", columns.getInt(11));
                column.put("autoincrement", columns.getString(23));
                column.put("primaryKey", pks.contains(column.get("name")));

                jsonArray.put(column);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataProcessingException();
        }

        return jsonArray;
    }

    /**
     * Method returns the statistics of specified database table column.
     *
     * @param dbInstanceName name of the database instance.
     * @param schemaName     name of the schema.
     * @param tableName      name of the table in the schema.
     * @param columnName     name of the column in the table.
     * @return JSONObject that contains average, minimal, maximal and median values of the column.
     * @throws DBConnectException      in case of database connection problem.
     * @throws DataProcessingException in case of problems during the data processing.
     */
    public JSONObject listTableStats(String dbInstanceName, String schemaName, String tableName, String columnName) throws DBConnectException, DataProcessingException {
        JSONObject result = new JSONObject();

        try (Connection connection = connectionFactory(dbInstanceName)) {

            Statement statement = connection.createStatement();


            String query = "SELECT AVG(" + columnName + ") FROM " + schemaName + "." + tableName;

            ResultSet avgSet = statement.executeQuery(query);

            while (avgSet.next()) {
                result.put("avg", avgSet.getObject(1));
            }


            query = "SELECT MIN(" + columnName + ") FROM " + schemaName + "." + tableName;

            ResultSet minSet = statement.executeQuery(query);

            while (minSet.next()) {
                result.put("min", minSet.getObject(1));
            }


            query = "SELECT MAX(" + columnName + ") FROM " + schemaName + "." + tableName;

            ResultSet maxSet = statement.executeQuery(query);

            while (maxSet.next()) {
                result.put("max", maxSet.getObject(1));
            }


            query = "SELECT PERCENTILE_DISC(0.5) within GROUP (ORDER BY " + tableName + "." + columnName + ")" +
                    " FROM " + schemaName + "." + tableName;

            ResultSet medianSet = statement.executeQuery(query);

            while (medianSet.next()) {
                result.put("median", medianSet.getObject(1));
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataProcessingException();
        }

        return result;
    }


    /**
     * Method return list of the columns that are used as primary key of the table.
     *
     * @param dbInstanceName name of the database instance.
     * @param schemaName     name of the schema.
     * @param tableName      name of the table in the schema.
     * @return List of the columns that are used as primary key of the table.
     * @throws DBConnectException      in case of database connection problem.
     * @throws DataProcessingException in case of problems during the data processing.
     */
    private List listPKs(String dbInstanceName, String schemaName, String tableName) throws DBConnectException, DataProcessingException {
        List<String> result = new ArrayList<>();

        try (Connection connection = connectionFactory(dbInstanceName)) {

            ResultSet columns = connection
                    .getMetaData()
                    .getPrimaryKeys(null, schemaName, tableName);

            while (columns.next()) {
                result.add(columns.getString(4));
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new DataProcessingException();
        }

        return result;
    }

    /**
     * Factory method for database connection creation.
     *
     * @param dbInstanceName name of the database instance.
     * @return
     */
    private Connection connectionFactory(String dbInstanceName) throws DBConnectException {
        try {
            Database database = databaseRepository.findByInstanceName(dbInstanceName);

            StringBuilder urlBuilder = new StringBuilder();

            urlBuilder.append("jdbc:postgresql://");
            urlBuilder.append(database.getHostname());
            urlBuilder.append(":");
            urlBuilder.append(database.getPort());
            urlBuilder.append("/");
            urlBuilder.append(database.getDatabaseName());

            String url = urlBuilder.toString();

            return DriverManager.getConnection(url, database.getUsername(), database.getPassword());
        } catch (SQLException e) {
            throw new DBConnectException("Could not connect to " + dbInstanceName + " database instance");
        } catch (NullPointerException e) {
            throw new DBConnectException("Database " + dbInstanceName + " does not exist");
        }
    }
}
