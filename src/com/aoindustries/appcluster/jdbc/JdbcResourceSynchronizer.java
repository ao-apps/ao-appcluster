/*
 * ao-appcluster - Coordinates system components installed in master/slave replication.
 * Copyright (C) 2011  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-appcluster.
 *
 * ao-appcluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-appcluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-appcluster.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.appcluster.jdbc;

import com.aoindustries.appcluster.CronResourceSynchronizer;
import com.aoindustries.appcluster.NodeDnsStatus;
import com.aoindustries.appcluster.ResourceNodeDnsResult;
import com.aoindustries.appcluster.ResourceStatus;
import com.aoindustries.appcluster.ResourceSynchronizationMode;
import com.aoindustries.appcluster.ResourceSynchronizationResult;
import com.aoindustries.appcluster.ResourceSynchronizationResultStep;
import com.aoindustries.cron.Schedule;
import com.aoindustries.sql.Catalog;
import com.aoindustries.sql.Column;
import com.aoindustries.sql.DatabaseMetaData;
import com.aoindustries.sql.Index;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.sql.Schema;
import com.aoindustries.sql.Table;
import com.aoindustries.util.Arrays;
import com.aoindustries.util.ErrorPrinter;
import com.aoindustries.util.StringUtility;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

/**
 * Performs synchronization using JDBC.
 *
 * TODO: Make no-warn tables, including public.user_login_keys
 *
 * TODO: Verify permissions?
 * TODO: Verify indexes?
 * TODO: Verify foreign keys?
 *
 * @author  AO Industries, Inc.
 */
public class JdbcResourceSynchronizer extends CronResourceSynchronizer<JdbcResource,JdbcResourceNode> {

    private static final int FETCH_SIZE = 1000;

    protected JdbcResourceSynchronizer(JdbcResourceNode localResourceNode, JdbcResourceNode remoteResourceNode, Schedule synchronizeSchedule, Schedule testSchedule) {
        super(localResourceNode, remoteResourceNode, synchronizeSchedule, testSchedule);
    }

    /*
     * May synchronize from a master to a slave.
     * May test from a master to a slave or a slave to a master.
     */
    @Override
    protected boolean canSynchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        NodeDnsStatus localDnsStatus = localDnsResult.getNodeStatus();
        NodeDnsStatus remoteDnsStatus = remoteDnsResult.getNodeStatus();
        switch(mode) {
            case SYNCHRONIZE :
                return
                    localDnsStatus==NodeDnsStatus.MASTER
                    && remoteDnsStatus==NodeDnsStatus.SLAVE
                ;
            case TEST_ONLY :
                return
                    (
                        localDnsStatus==NodeDnsStatus.MASTER
                        && remoteDnsStatus==NodeDnsStatus.SLAVE
                    ) || (
                        localDnsStatus==NodeDnsStatus.SLAVE
                        && remoteDnsStatus==NodeDnsStatus.MASTER
                    )
                ;
            default : throw new AssertionError("Unexpected mode: "+mode);
        }
    }

    /**
     * Gets the catalog.
     */
    private static Catalog getCatalog(DatabaseMetaData metaData) throws SQLException {
        SortedMap<String,Catalog> catalogs = metaData.getCatalogs();
        if(catalogs.isEmpty()) throw new SQLException(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.getCatalog.noRow"));
        if(catalogs.size()>1) throw new SQLException(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.getCatalog.moreThanOneRow"));
        return catalogs.get(catalogs.firstKey());
    }

    @Override
    protected ResourceSynchronizationResult synchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        final JdbcResource resource = localResourceNode.getResource();

        List<ResourceSynchronizationResultStep> steps = new ArrayList<ResourceSynchronizationResultStep>();

        // If exception is not caught within its own step, the error will be added with this step detail.
        // Each step should update this.
        long stepStartTime = System.currentTimeMillis();
        String step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.connect");
        StringBuilder stepOutput = new StringBuilder();
        StringBuilder stepWarning = new StringBuilder();
        StringBuilder stepError = new StringBuilder();

        try {
            // Will always synchronize or test from master to slave
            String fromDataSourceName;
            String toDataSourceName;
            NodeDnsStatus localDnsStatus = localDnsResult.getNodeStatus();
            NodeDnsStatus remoteDnsStatus = remoteDnsResult.getNodeStatus();
            switch(mode) {
                case SYNCHRONIZE :
                    if(
                        localDnsStatus==NodeDnsStatus.MASTER
                        && remoteDnsStatus==NodeDnsStatus.SLAVE
                    ) {
                        fromDataSourceName = localResourceNode.getDataSource();
                        toDataSourceName = remoteResourceNode.getDataSource();
                    } else {
                        throw new AssertionError();
                    }
                    break;
                case TEST_ONLY :
                    if(
                        localDnsStatus==NodeDnsStatus.MASTER
                        && remoteDnsStatus==NodeDnsStatus.SLAVE
                    ) {
                        fromDataSourceName = localResourceNode.getDataSource();
                        toDataSourceName = remoteResourceNode.getDataSource();
                    } else if(
                        localDnsStatus==NodeDnsStatus.SLAVE
                        && remoteDnsStatus==NodeDnsStatus.MASTER
                    ) {
                        fromDataSourceName = remoteResourceNode.getDataSource();
                        toDataSourceName = localResourceNode.getDataSource();
                    } else {
                        throw new AssertionError();
                    }
                    break;
                default : throw new AssertionError("Unexpected mode: "+mode);
            }
            stepOutput.append("fromDataSourceName: ").append(fromDataSourceName).append('\n');
            stepOutput.append("toDataSourceName..: ").append(toDataSourceName).append('\n');

            // Lookup the data sources
            Context ic = new InitialContext();
            Context envCtx = (Context)ic.lookup("java:comp/env");
            DataSource fromDataSource = (DataSource)envCtx.lookup(fromDataSourceName);
            if(fromDataSource==null) throw new NullPointerException("fromDataSource is null");
            DataSource toDataSource = (DataSource)envCtx.lookup(toDataSourceName);
            if(toDataSource==null) throw new NullPointerException("toDataSource is null");

            // Step #1: Connect to the data sources
            Connection fromConn = fromDataSource.getConnection();
            try {
                stepOutput.append("fromConn..........: ").append(fromConn).append('\n');
                fromConn.setReadOnly(true);
                fromConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                fromConn.setAutoCommit(false);

                Connection toConn = toDataSource.getConnection();
                try {
                    stepOutput.append("toConn............: ").append(toConn).append('\n');
                    toConn.setReadOnly(mode!=ResourceSynchronizationMode.SYNCHRONIZE);
                    toConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                    toConn.setAutoCommit(false);

                    // Connection successful
                    steps.add(new ResourceSynchronizationResultStep(stepStartTime, System.currentTimeMillis(), ResourceStatus.HEALTHY, step, stepOutput, stepWarning, stepError));

                    // Step #2 Compare meta data
                    stepStartTime = System.currentTimeMillis();
                    step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.compareMetaData");
                    stepOutput.setLength(0);
                    stepWarning.setLength(0);
                    stepError.setLength(0);

                    Catalog fromCatalog = getCatalog(new DatabaseMetaData(fromConn));
                    Catalog toCatalog = getCatalog(new DatabaseMetaData(toConn));

                    final Set<String> schemas = resource.getSchemas();
                    final Set<String> tableTypes = resource.getTableTypes();
                    final Set<String> excludeTables = resource.getExcludeTables();
                    compareSchemas(fromCatalog, toCatalog, schemas, tableTypes, excludeTables, stepError);

                    steps.add(
                        new ResourceSynchronizationResultStep(
                            stepStartTime,
                            System.currentTimeMillis(),
                            stepError.length()!=0 ? ResourceStatus.ERROR
                            : stepWarning.length()!=0 ? ResourceStatus.WARNING
                            : ResourceStatus.HEALTHY,
                            step,
                            stepOutput,
                            stepWarning,
                            stepError
                        )
                    );

                    // Only continue if all meta data is compatible
                    if(stepError.length()==0) {
                        if(mode==ResourceSynchronizationMode.TEST_ONLY) {
                            stepStartTime = System.currentTimeMillis();
                            step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.compareData");
                            stepOutput.setLength(0);
                            stepWarning.setLength(0);
                            stepError.setLength(0);

                            testSchemasData(fromConn, toConn, resource.getTestTimeout(), fromCatalog, toCatalog, schemas, tableTypes, excludeTables, resource.getNoWarnTables(), stepOutput, stepWarning);
                            steps.add(
                                new ResourceSynchronizationResultStep(
                                    stepStartTime,
                                    System.currentTimeMillis(),
                                    stepError.length()!=0 ? ResourceStatus.ERROR
                                    : stepWarning.length()!=0 ? ResourceStatus.WARNING
                                    : ResourceStatus.HEALTHY,
                                    step,
                                    stepOutput,
                                    stepWarning,
                                    stepError
                                )
                            );

                            // Nothing should have been changed, roll-back just to be safe
                            toConn.rollback();
                        } else if(mode==ResourceSynchronizationMode.SYNCHRONIZE) {
                            stepStartTime = System.currentTimeMillis();
                            step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.synchronizeData");
                            stepOutput.setLength(0);
                            stepWarning.setLength(0);
                            stepError.setLength(0);
                            // TODO: Synchronize data from here
                            // 1) Find all foreign keys
                            // 2) Topological sort to find dependency path
                            //    Error if any loops
                            // 3) Remove extra backwards
                            // 4) Update/insert forwards
                            // Everything OK, commit changes
                            toConn.commit();
                        } else {
                            throw new AssertionError("Unexpected mode: "+mode);
                        }
                    } else {
                        toConn.rollback();
                    }
                } catch(Throwable T) {
                    toConn.rollback();
                    throw T;
                } finally {
                    toConn.setAutoCommit(true);
                    toConn.close();
                }
            } finally {
                fromConn.rollback(); // Is read-only, this should always be OK and preferred to commit of accidental changes
                fromConn.setAutoCommit(true);
                fromConn.close();
            }
        } catch(ThreadDeath TD) {
            throw TD;
        } catch(Throwable T) {
            ErrorPrinter.printStackTraces(T, stepError);
            stepError.append('\n');
            steps.add(
                new ResourceSynchronizationResultStep(
                    stepStartTime,
                    System.currentTimeMillis(),
                    ResourceStatus.ERROR,
                    step,
                    stepOutput,
                    stepWarning,
                    stepError
                )
            );
        }

        return new ResourceSynchronizationResult(
            localResourceNode,
            remoteResourceNode,
            mode,
            steps
        );
    }

    private static void compareSchemas(
        Catalog fromCatalog,
        Catalog toCatalog,
        Set<String> schemas,
        Set<String> tableTypes,
        Set<String> excludeTables,
        StringBuilder stepError
    ) throws SQLException {
        for(String schema : schemas) {
            compareSchema(fromCatalog.getSchema(schema), toCatalog.getSchema(schema), tableTypes, excludeTables, stepError);
        }
    }

    private static void compareSchema(
        Schema fromSchema,
        Schema toSchema,
        Set<String> tableTypes,
        Set<String> excludeTables,
        StringBuilder stepError
    ) throws SQLException {
        SortedMap<String,Table> fromTables = fromSchema.getTables();
        SortedMap<String,Table> toTables = toSchema.getTables();

        // Get the union of all table names of included types that are not excluded
        SortedSet<String> allTableNames = new TreeSet<String>(DatabaseMetaData.getCollator());
        for(Table table : fromTables.values()) {
            String tableName = table.getName();
            if(
                !excludeTables.contains(fromSchema.getName()+'.'+tableName)
                && tableTypes.contains(table.getTableType())
            ) allTableNames.add(tableName);
        }
        for(Table table : toTables.values()) {
            String tableName = table.getName();
            if(
                !excludeTables.contains(toSchema.getName()+'.'+tableName)
                && tableTypes.contains(table.getTableType())
            ) allTableNames.add(tableName);
        }

        for(String tableName : allTableNames) {
            Table fromTable = fromTables.get(tableName);
            Table toTable = toTables.get(tableName);
            if(fromTable!=null) {
                if(toTable!=null) {
                    // Exists in both, continue on to check columns
                    compareTable(fromTable, toTable, stepError);
                } else {
                    stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareSchema.missingTable", toSchema.getCatalog(), toSchema, tableName)).append('\n');
                }
            } else {
                if(toTable!=null) {
                    stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareSchema.extraTable", toSchema.getCatalog(), toSchema, tableName)).append('\n');
                } else {
                    throw new AssertionError("Must exist in at least one of the sets");
                }
            }
        }
    }

    private static void compareTable(Table fromTable, Table toTable, StringBuilder stepError) throws SQLException {
        assert fromTable.equals(toTable);
        if(!fromTable.getTableType().equals(toTable.getTableType())) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareTable.mismatchedType",
                    fromTable.getSchema(),
                    fromTable,
                    fromTable.getTableType(),
                    toTable.getTableType()
                )
            ).append('\n');
        } else {
            // Compare columns
            SortedMap<String,Column> fromColumns = fromTable.getColumnMap();
            SortedMap<String,Column> toColumns = toTable.getColumnMap();
            SortedSet<String> allColumnNames = new TreeSet<String>(DatabaseMetaData.getCollator());
            allColumnNames.addAll(fromColumns.keySet());
            allColumnNames.addAll(toColumns.keySet());
            for(String columnName : allColumnNames) {
                Column fromColumn = fromColumns.get(columnName);
                Column toColumn = toColumns.get(columnName);
                if(fromColumn!=null) {
                    if(toColumn!=null) {
                        // Exists in both, continue on to check column detail
                        compareColumn(fromColumn, toColumn, stepError);
                    } else {
                        stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareTable.missingColumn", toTable.getSchema().getCatalog(), toTable.getSchema(), toTable, columnName)).append('\n');
                    }
                } else {
                    if(toColumn!=null) {
                        stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareTable.extraColumn", toTable.getSchema().getCatalog(), toTable.getSchema(), toTable, columnName)).append('\n');
                    } else {
                        throw new AssertionError("Must exist in at least one of the sets");
                    }
                }
            }
            // Compare primary key
            comparePrimaryKey(fromTable.getPrimaryKey(), toTable.getPrimaryKey(), stepError);
        }
    }

    private static void compareColumn(Column fromColumn, Column toColumn, StringBuilder stepError) {
        String schema = fromColumn.getTable().getSchema().getName();
        String table = fromColumn.getTable().getName();
        String column = fromColumn.getName();
        // int typeName
        if(fromColumn.getDataType()!=toColumn.getDataType()) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.dataType",
                    schema,
                    table,
                    column,
                    fromColumn.getDataType(),
                    toColumn.getDataType()
                )
            ).append('\n');
        }
        // String typeName
        if(!StringUtility.equals(fromColumn.getTypeName(), toColumn.getTypeName())) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.typeName",
                    schema,
                    table,
                    column,
                    fromColumn.getTypeName(),
                    toColumn.getTypeName()
                )
            ).append('\n');
        }
        // Integer columnSize
        if(!StringUtility.equals(fromColumn.getColumnSize(), toColumn.getColumnSize())) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.columnSize",
                    schema,
                    table,
                    column,
                    fromColumn.getColumnSize(),
                    toColumn.getColumnSize()
                )
            ).append('\n');
        }
        // Integer decimalDigits
        if(!StringUtility.equals(fromColumn.getDecimalDigits(), toColumn.getDecimalDigits())) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.decimalDigits",
                    schema,
                    table,
                    column,
                    fromColumn.getDecimalDigits(),
                    toColumn.getDecimalDigits()
                )
            ).append('\n');
        }
        // int nullable
        if(fromColumn.getNullable()!=toColumn.getNullable()) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.nullable",
                    schema,
                    table,
                    column,
                    fromColumn.getNullable(),
                    toColumn.getNullable()
                )
            ).append('\n');
        }
        // String columnDef
        if(!StringUtility.equals(fromColumn.getColumnDef(), toColumn.getColumnDef())) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.columnDef",
                    schema,
                    table,
                    column,
                    fromColumn.getColumnDef(),
                    toColumn.getColumnDef()
                )
            ).append('\n');
        }
        // Integer charOctetLength
        if(!StringUtility.equals(fromColumn.getCharOctetLength(), toColumn.getCharOctetLength())) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.charOctetLength",
                    schema,
                    table,
                    column,
                    fromColumn.getCharOctetLength(),
                    toColumn.getCharOctetLength()
                )
            ).append('\n');
        }
        // int ordinalPosition
        if(fromColumn.getOrdinalPosition()!=toColumn.getOrdinalPosition()) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.ordinalPosition",
                    schema,
                    table,
                    column,
                    fromColumn.getOrdinalPosition(),
                    toColumn.getOrdinalPosition()
                )
            ).append('\n');
        }
        // String isNullable
        if(!StringUtility.equals(fromColumn.getIsNullable(), toColumn.getIsNullable())) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.isNullable",
                    schema,
                    table,
                    column,
                    fromColumn.getIsNullable(),
                    toColumn.getIsNullable()
                )
            ).append('\n');
        }
        // String isAutoincrement
        if(!StringUtility.equals(fromColumn.getIsAutoincrement(), toColumn.getIsAutoincrement())) {
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.compareColumn.mismatch.isAutoincrement",
                    schema,
                    table,
                    column,
                    fromColumn.getIsAutoincrement(),
                    toColumn.getIsAutoincrement()
                )
            ).append('\n');
        }
    }

    private static void comparePrimaryKey(Index fromPrimaryKey, Index toPrimaryKey, StringBuilder stepError) {
        if(fromPrimaryKey==null) {
            Table fromTable = fromPrimaryKey.getTable();
            Schema fromSchema = fromTable.getSchema();
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.comparePrimaryKey.primaryKeyMissing",
                    fromSchema.getCatalog(),
                    fromSchema,
                    fromTable
                )
            ).append('\n');
        }
        if(toPrimaryKey==null) {
            Table toTable = toPrimaryKey.getTable();
            Schema toSchema = toTable.getSchema();
            stepError.append(
                ApplicationResources.accessor.getMessage(
                    "JdbcResourceSynchronizer.comparePrimaryKey.primaryKeyMissing",
                    toSchema.getCatalog(),
                    toSchema,
                    toTable
                )
            ).append('\n');
        }
        if(fromPrimaryKey!=null && toPrimaryKey!=null) {
            if(!StringUtility.equals(fromPrimaryKey.getName(), toPrimaryKey.getName())) {
                stepError.append(
                    ApplicationResources.accessor.getMessage(
                        "JdbcResourceSynchronizer.comparePrimaryKey.mismatch.name",
                        fromPrimaryKey.getTable().getSchema(),
                        fromPrimaryKey.getTable(),
                        fromPrimaryKey.getName(),
                        toPrimaryKey.getName()
                    )
                ).append('\n');
            }
            if(!fromPrimaryKey.getColumns().equals(toPrimaryKey.getColumns())) {
                stepError.append(
                    ApplicationResources.accessor.getMessage(
                        "JdbcResourceSynchronizer.comparePrimaryKey.mismatch.columns",
                        fromPrimaryKey.getTable().getSchema(),
                        fromPrimaryKey.getTable(),
                        "(" + StringUtility.join(fromPrimaryKey.getColumns(), ", ") + ")",
                        "(" + StringUtility.join(toPrimaryKey.getColumns(), ", ") + ")"
                    )
                ).append('\n');
            }
        }
    }

    private static void testSchemasData(Connection fromConn, Connection toConn, int timeout, Catalog fromCatalog, Catalog toCatalog, Set<String> schemas, Set<String> tableTypes, Set<String> excludeTables, Set<String> noWarnTables, StringBuilder stepOutput, StringBuilder stepWarning) throws SQLException {
        List<Object> outputTable = new ArrayList<Object>();
        try {
            for(String schema : schemas) {
                testSchemaData(fromConn, toConn, timeout, fromCatalog.getSchema(schema), toCatalog.getSchema(schema), tableTypes, excludeTables, noWarnTables, outputTable, stepOutput, stepWarning);
            }
        } finally {
            try {
                // Insert the table before any other output
                String currentOut = stepOutput.toString();
                stepOutput.setLength(0);
                SQLUtility.printTable(
                    new String[] {
                        ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.schema"),
                        ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.table"),
                        ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.matches"),
                        ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.modified"),
                        ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.missing"),
                        ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.extra"),
                    },
                    outputTable.toArray(),
                    stepOutput,
                    true,
                    new boolean[] {
                        false,
                        false,
                        true,
                        true,
                        true,
                        true
                    }
                );
                //stepOutput.append('\n');
                stepOutput.append(currentOut);
            } catch(IOException exc) {
                throw new AssertionError(exc);
            }
        }
    }

    private static void testSchemaData(
        Connection fromConn,
        Connection toConn,
        int timeout,
        Schema fromSchema,
        Schema toSchema,
        Set<String> tableTypes,
        Set<String> excludeTables,
        Set<String> noWarnTables,
        List<Object> outputTable,
        StringBuilder stepOutput,
        StringBuilder stepWarning
    ) throws SQLException {
        assert fromSchema.equals(toSchema);

        SortedMap<String,Table> fromTables = fromSchema.getTables();
        SortedMap<String,Table> toTables = toSchema.getTables();

        assert fromTables.keySet().equals(toTables.keySet()) : "This should have been caught by the meta data checks";

        for(String tableName : fromTables.keySet()) {
            Table fromTable = fromTables.get(tableName);
            Table toTable = toTables.get(tableName);
            String tableType = fromTable.getTableType();
            assert tableType.equals(toTable.getTableType()) : "This should have been caught by the meta data checks";
            if(
                !excludeTables.contains(fromSchema.getName()+'.'+tableName)
                && tableTypes.contains(tableType)
            ) {
                if("TABLE".equals(tableType)) testTableData(fromConn, toConn, timeout, fromTable, toTable, noWarnTables, outputTable, stepOutput, stepWarning);
                else throw new SQLException("Unimplemented table type: " + tableType);
            }
        }
    }

    /**
     * Appends a value in a per-type way.
     */
    private static void appendValue(StringBuilder sb, int dataType, Object value) {
        if(value==null) {
            sb.append("NULL");
        } else {
            switch(dataType) {
                // Types without quotes
                case Types.BIGINT :
                case Types.BIT :
                case Types.BOOLEAN :
                case Types.DOUBLE :
                case Types.FLOAT :
                case Types.INTEGER :
                case Types.NULL :
                case Types.REAL :
                case Types.SMALLINT :
                case Types.TINYINT :
                    sb.append(value.toString());
                    break;
                // All other types will use quotes
                default :
                    try {
                        sb.append('\'');
                        StringUtility.replace(value.toString(), "'", "''", sb);
                        sb.append('\'');
                    } catch(IOException exc) {
                        throw new AssertionError(exc); // Should not happen
                    }
            }
        }
    }

    /**
     * A row encapsulates one set of results.
     */
    static class Row implements Comparable<Row> {

        private final Column[] primaryKeyColumns;
        private final Column[] nonPrimaryKeyColumns;
        private final Object[] values;

        Row(Column[] primaryKeyColumns, Column[] nonPrimaryKeyColumns, Object[] values) {
            this.primaryKeyColumns = primaryKeyColumns;
            this.nonPrimaryKeyColumns = nonPrimaryKeyColumns;
            this.values = values;
        }

        /**
         * Orders rows by the values of the primary key columns.
         * Orders rows in the same exact way as the underlying database to make
         * sure that the one-pass comparison is correct.
         * Only returns zero when the primary key values are an exact match.
         * The comparison is consistent with equals for all primary key values.
         */
        @Override
        public int compareTo(Row other) {
            for(Column primaryKeyColumn : primaryKeyColumns) {
                int index = primaryKeyColumn.getOrdinalPosition() - 1;
                Object val = values[index];
                Object otherVal = other.values[index];
                int diff;
                switch(primaryKeyColumn.getDataType()) {
                    case Types.BIGINT :
                        diff = ((Long)val).compareTo((Long)otherVal);
                        break;
                    // These were converted to UTF8 byte[] during order by.
                    // Use the same conversion here
                    case Types.CHAR :
                    case Types.VARCHAR :
                        try {
                            //diff = ((String)val).compareTo((String)otherVal);
                            diff = Arrays.compare(
                                ((String)val).getBytes("UTF-8"),
                                ((String)otherVal).getBytes("UTF-8")
                            );
                        } catch(UnsupportedEncodingException exc) {
                            throw new RuntimeException(exc);
                        }
                        break;
                    case Types.DATE :
                        diff = ((Date)val).compareTo((Date)otherVal);
                        break;
                    case Types.DECIMAL :
                    case Types.NUMERIC :
                        diff = ((BigDecimal)val).compareTo((BigDecimal)otherVal);
                        break;
                    case Types.DOUBLE :
                        diff = ((Double)val).compareTo((Double)otherVal);
                        break;
                    case Types.FLOAT :
                        diff = ((Float)val).compareTo((Float)otherVal);
                        break;
                    case Types.INTEGER :
                        diff = ((Integer)val).compareTo((Integer)otherVal);
                        break;
                    case Types.SMALLINT :
                        diff = ((Short)val).compareTo((Short)otherVal);
                        break;
                    case Types.TIME :
                        diff = ((Time)val).compareTo((Time)otherVal);
                        break;
                    case Types.TIMESTAMP :
                        diff = ((Timestamp)val).compareTo((Timestamp)otherVal);
                        break;
                    default : throw new UnsupportedOperationException("Type comparison not implemented: "+primaryKeyColumn.getDataType());
                }
                assert (diff==0) == (val.equals(otherVal)) : "Not consistent with equals: val="+val+", otherVal="+otherVal;
                if(diff!=0) return diff;
            }
            return 0; // Exact match
        }

        boolean equalsNonPrimaryKey(Row other) {
            for(Column nonPrimaryKeyColumn : nonPrimaryKeyColumns) {
                int index = nonPrimaryKeyColumn.getOrdinalPosition() - 1;
                if(
                    !StringUtility.equals(
                        values[index],
                        other.values[index]
                    )
                ) return false;
            }
            return true;
        }

        /**
         * Gets a string representation of the primary key values of this row.
         * This is only meant to be human readable, not for sending to SQL directly.
         */
        String getPrimaryKeyValues() {
            StringBuilder sb = new StringBuilder();
            sb.append('(');
            boolean didOne = false;
            for(Column primaryKeyColumn : primaryKeyColumns) {
                if(didOne) sb.append(", ");
                else didOne = true;
                appendValue(sb, primaryKeyColumn.getDataType(), values[primaryKeyColumn.getOrdinalPosition()-1]);
            }
            sb.append(')');
            return sb.toString();
        }
    }

    /**
     * Iterates rows from a result set, ensuring that each row is properly ordered after the previous.
     */
    static class RowIterator {
        private final Column[] columns;
        private final Column[] primaryKeyColumns;
        private final Column[] nonPrimaryKeyColumns;
        private final ResultSet results;
        private Row previousRow;
        private Row nextRow;

        RowIterator(List<Column> columns, Index primaryKey, ResultSet results) throws SQLException {
            this.columns = columns.toArray(new Column[columns.size()]);
            List<Column> pkColumns = primaryKey.getColumns();
            this.primaryKeyColumns = pkColumns.toArray(new Column[pkColumns.size()]);
            nonPrimaryKeyColumns = new Column[columns.size() - pkColumns.size()];
            int index = 0;
            for(Column column : this.columns) {
                if(!pkColumns.contains(column)) nonPrimaryKeyColumns[index++] = column;
            }
            if(index!=nonPrimaryKeyColumns.length) throw new AssertionError();
            this.results = results;
            this.nextRow = getNextRow();
        }

        /**
         * Gest the next row from the results.
         */
        private Row getNextRow() throws SQLException {
            if(results.next()) {
                Object[] values = new Object[columns.length];
                for(int index=0; index<values.length; index++) values[index] = results.getObject(index+1);
                return new Row(primaryKeyColumns, nonPrimaryKeyColumns, values);
            } else {
                return null;
            }
        }

        /**
         * Gets and does not remove the next row.
         *
         * @return  the next row or <code>null</code> when all rows have been read.
         */
        Row peek() {
            return nextRow;
        }

        /**
         * Gets and removes the next row.
         *
         * @return  the next row or <code>null</code> when all rows have been read.
         */
        Row remove() throws SQLException {
            if(nextRow==null) return nextRow;
            Row newNextRow = getNextRow();
            if(newNextRow==null) {
                previousRow = null;
                nextRow = null;
                return null;
            }
            if(previousRow!=null) {
                // Make sure this row is after the previous
                if(newNextRow.compareTo(previousRow)<=0) throw new SQLException("Rows out of order: " + previousRow.getPrimaryKeyValues() + " and " + newNextRow.getPrimaryKeyValues());
            }
            previousRow = nextRow;
            nextRow = newNextRow;
            return newNextRow;
        }
    }

    /**
     * Queries both from and to tables, sorted by each column of the primary key in ascending order.
     * All differences are found in a single pass through the tables, with no buffering and only a single query of each result.
     */
    private static void testTableData(
        Connection fromConn,
        Connection toConn,
        int timeout,
        Table fromTable,
        Table toTable,
        Set<String> noWarnTables,
        List<Object> outputTable,
        StringBuilder stepOutput,
        StringBuilder stepWarning
    ) throws SQLException {
        assert fromTable.equals(toTable);
        final String schema = fromTable.getSchema().getName();
        final StringBuilder stepResults = noWarnTables.contains(schema+'.'+fromTable.getName()) ? stepOutput : stepWarning;
        List<Column> columns = fromTable.getColumns();
        Index primaryKey = fromTable.getPrimaryKey();
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        boolean didOne = false;
        for(Column column : columns) {
            if(didOne) sql.append(", ");
            else didOne = true;
            switch(column.getDataType()) {
                // These will be verified using md5
                case Types.BINARY :
                case Types.BLOB :
                case Types.LONGVARBINARY :
                case Types.VARBINARY :
                    sql.append(" md5(\"").append(column.getName()).append("\")");
                    break;
                // All others are fully compared
                default :
                    sql.append('"').append(column.getName()).append('"');
            }
        }
        sql.append(" FROM \"").append(schema).append("\".\"").append(fromTable.getName()).append("\" ORDER BY ");
        didOne = false;
        for(Column column : primaryKey.getColumns()) {
            if(didOne) sql.append(", ");
            else didOne = true;
            switch(column.getDataType()) {
                // These will be verified using md5
                case Types.BINARY :
                case Types.BLOB :
                case Types.LONGVARBINARY :
                case Types.VARBINARY :
                    throw new SQLException("Type not supported in primary key: "+column.getDataType());
                // These will be converted to UTF8 bytea for collator-neutral ordering (not dependent on PostgreSQL lc_collate setting)
                case Types.CHAR :
                case Types.VARCHAR :
                    sql.append("convert_to(\"").append(column.getName()).append("\", 'UTF8')");
                    break;
                // All others are compared directly
                default :
                    sql.append('"').append(column.getName()).append('"');
            }
        }
        String sqlString = sql.toString();
        Statement fromStmt = fromConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
        try {
            fromStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
            fromStmt.setFetchSize(FETCH_SIZE);
            // Not yet implemented by PostgreSQL: fromStmt.setPoolable(false);
            // Not yet implemented by PostgreSQL: fromStmt.setQueryTimeout(timeout);
            Statement toStmt = toConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            try {
                toStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
                toStmt.setFetchSize(FETCH_SIZE);
                // Not yet implemented by PostgreSQL: toStmt.setPoolable(false);
                // Not yet implemented by PostgreSQL: toStmt.setQueryTimeout(timeout);
                ResultSet fromResults = fromStmt.executeQuery(sqlString);
                try {
                    ResultSet toResults = toStmt.executeQuery(sqlString);
                    try {
                        long matches = 0;
                        long modified = 0;
                        long missing = 0;
                        long extra = 0;
                        RowIterator fromIter = new RowIterator(columns, primaryKey, fromResults);
                        RowIterator toIter = new RowIterator(columns, primaryKey, toResults);
                        while(true) {
                            Row fromRow = fromIter.peek();
                            Row toRow = toIter.peek();
                            if(fromRow!=null) {
                                if(toRow!=null) {
                                    int primaryKeyDiff = fromRow.compareTo(toRow);
                                    if(primaryKeyDiff==0) {
                                        // Primary keys have already been compared and are known to be equal, only need to compare the remaining columns
                                        if(fromRow.equalsNonPrimaryKey(toRow)) {
                                            // Exact match, remove both
                                            matches++;
                                            fromIter.remove();
                                            toIter.remove();
                                        } else {
                                            // Modified
                                            stepResults.append(
                                                ApplicationResources.accessor.getMessage(
                                                    "JdbcResourceSynchronizer.testTableData.modified",
                                                    schema,
                                                    fromTable,
                                                    fromRow.getPrimaryKeyValues()
                                                )
                                            ).append('\n');
                                            modified++;
                                            fromIter.remove();
                                            toIter.remove();
                                        }
                                    } else if(primaryKeyDiff<0) {
                                        // Missing
                                        stepResults.append(
                                            ApplicationResources.accessor.getMessage(
                                                "JdbcResourceSynchronizer.testTableData.missing",
                                                schema,
                                                fromTable,
                                                fromRow.getPrimaryKeyValues()
                                            )
                                        ).append('\n');
                                        missing++;
                                        fromIter.remove();
                                    } else {
                                        assert primaryKeyDiff>0;
                                        // Extra
                                        stepResults.append(
                                            ApplicationResources.accessor.getMessage(
                                                "JdbcResourceSynchronizer.testTableData.extra",
                                                schema,
                                                toTable,
                                                toRow.getPrimaryKeyValues()
                                            )
                                        ).append('\n');
                                        extra++;
                                        toIter.remove();
                                    }
                                } else {
                                    // Missing
                                    stepResults.append(
                                        ApplicationResources.accessor.getMessage(
                                            "JdbcResourceSynchronizer.testTableData.missing",
                                            schema,
                                            fromTable,
                                            fromRow.getPrimaryKeyValues()
                                        )
                                    ).append('\n');
                                    missing++;
                                    fromIter.remove();
                                }
                            } else {
                                if(toRow!=null) {
                                    // Extra
                                    stepResults.append(
                                        ApplicationResources.accessor.getMessage(
                                            "JdbcResourceSynchronizer.testTableData.extra",
                                            schema,
                                            toTable,
                                            toRow.getPrimaryKeyValues()
                                        )
                                    ).append('\n');
                                    extra++;
                                    toIter.remove();
                                } else {
                                    // All rows done
                                    break;
                                }
                            }
                        }
                        // Add totals to stepOutput
                        outputTable.add(schema);
                        outputTable.add(fromTable.getName());
                        outputTable.add(matches);
                        outputTable.add(modified==0 ? null : modified);
                        outputTable.add(missing==0 ? null : missing);
                        outputTable.add(extra==0 ? null : extra);
                    } finally {
                        toResults.close();
                    }
                } finally {
                    fromResults.close();
                }
            } finally {
                toStmt.close();
            }
        } finally {
            fromStmt.close();
        }
    }
}
