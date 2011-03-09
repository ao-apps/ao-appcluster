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
import com.aoindustries.sql.Schema;
import com.aoindustries.sql.Table;
import com.aoindustries.util.ErrorPrinter;
import com.aoindustries.util.StringUtility;
import java.sql.Connection;
import java.sql.SQLException;
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
 * @author  AO Industries, Inc.
 */
public class JdbcResourceSynchronizer extends CronResourceSynchronizer<JdbcResource,JdbcResourceNode> {

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
                    steps.add(new ResourceSynchronizationResultStep(stepStartTime, System.currentTimeMillis(), ResourceStatus.HEALTHY, step, stepOutput, stepError));

                    // Step #2 Compare meta data
                    stepStartTime = System.currentTimeMillis();
                    step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.compareMetaData");
                    stepOutput.setLength(0);
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
                            stepError.length()==0 ? ResourceStatus.HEALTHY : ResourceStatus.ERROR,
                            step,
                            stepOutput,
                            stepError
                        )
                    );

                    // Only continue if all meta data is compatible
                    if(stepError.length()==0) {
                        if(mode==ResourceSynchronizationMode.TEST_ONLY) {
                            stepStartTime = System.currentTimeMillis();
                            step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.compareData");
                            stepOutput.setLength(0);
                            stepError.setLength(0);
                            // TODO: Test data from here
                            toConn.rollback();
                        } else if(mode==ResourceSynchronizationMode.SYNCHRONIZE) {
                            stepStartTime = System.currentTimeMillis();
                            step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.synchronizeData");
                            stepOutput.setLength(0);
                            stepError.setLength(0);
                            // TODO: Synchronize data from here
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
        assert fromTable.getSchema().getName().equals(toTable.getSchema().getName());
        assert fromTable.getName().equals(toTable.getName());
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
            SortedMap<String,Column> fromColumns = fromTable.getColumns();
            SortedMap<String,Column> toColumns = toTable.getColumns();
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
}
