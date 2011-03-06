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
import com.aoindustries.appcluster.ResourceSynchronizationResult;
import com.aoindustries.appcluster.ResourceTestResult;
import com.aoindustries.cron.Schedule;

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
     */
    @Override
    protected boolean canSynchronize(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        return
            localDnsResult.getNodeStatus()==NodeDnsStatus.MASTER
            && remoteDnsResult.getNodeStatus()==NodeDnsStatus.SLAVE
        ;
    }

    @Override
    protected ResourceSynchronizationResult synchronize(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        long startTime = System.currentTimeMillis();
        System.err.println(this+": synchronize: TODO");
        return new ResourceSynchronizationResult(startTime, System.currentTimeMillis(), ResourceStatus.HEALTHY, null, null);
    }

    /*
     * May test from a master to a slave or a slave to a master.
     */
    @Override
    protected boolean canTest(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        NodeDnsStatus localDnsStatus = localDnsResult.getNodeStatus();
        NodeDnsStatus remoteDnsStatus = remoteDnsResult.getNodeStatus();
        return
            (
                localDnsStatus==NodeDnsStatus.MASTER
                && remoteDnsStatus==NodeDnsStatus.SLAVE
            ) || (
                localDnsStatus==NodeDnsStatus.SLAVE
                && remoteDnsStatus==NodeDnsStatus.MASTER
            )
        ;
    }

    @Override
    protected ResourceTestResult test(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        long startTime = System.currentTimeMillis();
        System.err.println(this+": test: TODO");
        return new ResourceTestResult(startTime, System.currentTimeMillis(), ResourceStatus.HEALTHY, null, null);
    }
}
