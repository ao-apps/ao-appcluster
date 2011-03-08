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
import com.aoindustries.util.ErrorPrinter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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

    private static final Collator englishCollator = Collator.getInstance(Locale.ENGLISH);

    /**
     * Gets the catalog.
     */
    private static String getCatalog(DatabaseMetaData metaData) throws SQLException {
        ResultSet results = metaData.getCatalogs();
        try {
            if(!results.next()) throw new SQLException(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.getCatalog.noRow"));
            String catalog = results.getString(1);
            if(results.next()) throw new SQLException(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.getCatalog.moreThanOneRow"));
            return catalog;
        } finally {
            results.close();
        }
    }

    /**
     * Gets the schemas for the provided catalog.
     */
    private static SortedSet<String> getSchemas(DatabaseMetaData metaData, String catalog) throws SQLException {
        ResultSet results = metaData.getSchemas(catalog, null);
        try {
            SortedSet<String> schemas = new TreeSet<String>(englishCollator);
            while(results.next()) schemas.add(results.getString(1));
            return schemas;
        } finally {
            results.close();
        }
    }

    @Override
    protected ResourceSynchronizationResult synchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        List<ResourceSynchronizationResultStep> steps = new ArrayList<ResourceSynchronizationResultStep>();

        // If exception is not caught within its own step, the error will be added with this step detail.
        // Each step should update this.
        long stepStartTime = System.currentTimeMillis();
        String step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.connect");
        List<String> stepOutputs = new ArrayList<String>();
        List<String> stepErrors = new ArrayList<String>();

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
            stepOutputs.add(
                "fromDataSourceName="+fromDataSourceName+"\n"
                + "toDataSourceName="+toDataSourceName
            );

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
                Connection toConn = toDataSource.getConnection();
                try {
                    stepOutputs.add(
                        "fromConn="+fromConn+"\n"
                        + "toConn="+toConn
                    );
                    // Connection successful
                    steps.add(new ResourceSynchronizationResultStep(stepStartTime, System.currentTimeMillis(), ResourceStatus.HEALTHY, step, stepOutputs, stepErrors));

                    // Step #2 Compare meta data
                    stepStartTime = System.currentTimeMillis();
                    step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.compareMetaData");
                    stepOutputs.clear();
                    stepErrors.clear();

                    DatabaseMetaData fromDbMeta = fromConn.getMetaData();
                    DatabaseMetaData toDbMeta = toConn.getMetaData();

                    String fromCatalog = getCatalog(fromDbMeta);
                    String toCatalog = getCatalog(toDbMeta);
                    stepOutputs.add(
                        "fromCatalog="+fromCatalog+"\n"
                        + "toCatalog="+toCatalog
                    );

                    SortedSet<String> fromSchemas = getSchemas(fromDbMeta, fromCatalog);
                    SortedSet<String> toSchemas = getSchemas(toDbMeta, toCatalog);
                    stepOutputs.add(
                        "fromSchemas="+fromSchemas.toString()+"\n"
                        + "toCatalogs="+toSchemas.toString()
                    );

                    // TODO
                    steps.add(new ResourceSynchronizationResultStep(stepStartTime, System.currentTimeMillis(), ResourceStatus.HEALTHY, step, stepOutputs, stepErrors));
                } finally {
                    toConn.close();
                }
            } finally {
                fromConn.close();
            }
        } catch(ThreadDeath TD) {
            throw TD;
        } catch(Throwable T) {
            stepErrors.add(ErrorPrinter.getStackTraces(T));
            steps.add(
                new ResourceSynchronizationResultStep(
                    stepStartTime,
                    System.currentTimeMillis(),
                    ResourceStatus.ERROR,
                    step,
                    stepOutputs,
                    stepErrors
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
}
