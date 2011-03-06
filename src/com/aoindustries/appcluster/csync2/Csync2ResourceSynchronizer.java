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
package com.aoindustries.appcluster.csync2;

import com.aoindustries.appcluster.CronResourceSynchronizer;
import com.aoindustries.appcluster.NodeDnsStatus;
import com.aoindustries.appcluster.ResourceNodeDnsResult;
import com.aoindustries.appcluster.ResourceSynchronizationResult;
import com.aoindustries.appcluster.ResourceTestResult;
import com.aoindustries.cron.Schedule;

/**
 * Performs synchronization using csync2.
 *
 * @author  AO Industries, Inc.
 */
public class Csync2ResourceSynchronizer extends CronResourceSynchronizer<Csync2Resource,Csync2ResourceNode> {

    protected Csync2ResourceSynchronizer(Csync2ResourceNode localResourceNode, Csync2ResourceNode remoteResourceNode, Schedule synchronizeSchedule, Schedule testSchedule) {
        super(localResourceNode, remoteResourceNode, synchronizeSchedule, testSchedule);
    }

    /*
     * May synchronize from any master or slave to any master or slave.
     */
    @Override
    protected boolean canSynchronize(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        NodeDnsStatus localDnsStatus = localDnsResult.getNodeStatus();
        NodeDnsStatus remoteDnsStatus = remoteDnsResult.getNodeStatus();
        return
            (
                localDnsStatus==NodeDnsStatus.MASTER
                || localDnsStatus==NodeDnsStatus.SLAVE
            ) && (
                remoteDnsStatus==NodeDnsStatus.MASTER
                || remoteDnsStatus==NodeDnsStatus.SLAVE
            )
        ;
    }

    @Override
    protected ResourceSynchronizationResult synchronize(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        System.err.println(this+": synchronize: TODO");
        return new ResourceSynchronizationResult();
    }

    /*
     * May test from any master or slave to any master or slave.
     */
    @Override
    protected boolean canTest(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        NodeDnsStatus localDnsStatus = localDnsResult.getNodeStatus();
        NodeDnsStatus remoteDnsStatus = remoteDnsResult.getNodeStatus();
        return
            (
                localDnsStatus==NodeDnsStatus.MASTER
                || localDnsStatus==NodeDnsStatus.SLAVE
            ) && (
                remoteDnsStatus==NodeDnsStatus.MASTER
                || remoteDnsStatus==NodeDnsStatus.SLAVE
            )
        ;
    }

    @Override
    protected ResourceTestResult test(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        System.err.println(this+": test: TODO");
        return new ResourceTestResult();
    }
}
