package com.ataccama.homework.rest;

import com.ataccama.homework.service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
public class DatabaseController {

    private final DatabaseService databaseService;

    @Autowired
    public DatabaseController(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }


    /**
     * Method returns all schemas in database instance.
     *
     * @param dbInstanceName name of the database instance.
     * @return The list of all schemas or an error message.
     */
    @GetMapping(value = "/{dbInstanceName}", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity listSchemas(@PathVariable String dbInstanceName) {
        try {
            String JSONResponse = databaseService.listSchemas(dbInstanceName).toString();
            return new ResponseEntity<>(JSONResponse, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Method returns names of the tables located in the specified schema.
     *
     * @param dbInstanceName name of the database instance.
     * @param schemaName     name of the schema.
     * @return The list of the tables in the schema or an error message.
     */
    @GetMapping(value = "/{dbInstanceName}/{schemaName}", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity listTablesInSchema(@PathVariable String dbInstanceName,
                                             @PathVariable String schemaName) {
        try {
            String JSONResponse = databaseService.listTables(dbInstanceName, schemaName).toString();
            return new ResponseEntity<>(JSONResponse, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Method returns data of specified database table.
     *
     * @param dbInstanceName name of the database instance.
     * @param schemaName     name of the schema.
     * @param tableName      name of the table.
     * @return The data of the table or an error message.
     */
    @GetMapping(value = "/{dbInstanceName}/{schemaName}/{tableName}", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity listTableData(@PathVariable String dbInstanceName,
                                        @PathVariable String schemaName,
                                        @PathVariable String tableName) {
        try {
            String JSONResponse = databaseService.getTableData(dbInstanceName, schemaName, tableName).toString();
            return new ResponseEntity<>(JSONResponse, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Method returns description of the database table columns.
     *
     * @param dbInstanceName name of the database instance.
     * @param schemaName     name of the schema.
     * @param tableName      name of the table.
     * @return the list of the columns or an error message.
     */
    @GetMapping(value = "/{dbInstanceName}/{schemaName}/{tableName}/columns", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity listTableColumns(@PathVariable String dbInstanceName,
                                           @PathVariable String schemaName,
                                           @PathVariable String tableName) {
        try {
            String JSONResponse = databaseService.listColumns(dbInstanceName, schemaName, tableName).toString();
            return new ResponseEntity<>(JSONResponse, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Method returns the statistics of specified database table column.
     *
     * @param dbInstanceName name of the database instance.
     * @param schemaName     name of the schema.
     * @param tableName      name of the table in the schema.
     * @param columnName     name of the column in the table.
     * @return JSON formatted string that containsaverage, minimal, maximal and median values of the column or an error message.
     */
    @GetMapping(value = "/{dbInstanceName}/{schemaName}/{tableName}/columns/{columnName}", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity listTableStats(@PathVariable String dbInstanceName,
                                         @PathVariable String schemaName,
                                         @PathVariable String tableName,
                                         @PathVariable String columnName) {
        try {
            String JSONResponse = databaseService.listTableStats(dbInstanceName, schemaName, tableName, columnName).toString();
            return new ResponseEntity<>(JSONResponse, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
