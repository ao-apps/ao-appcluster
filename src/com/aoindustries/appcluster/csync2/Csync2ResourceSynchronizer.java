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
import com.aoindustries.appcluster.ResourceStatus;
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
        long startTime = System.currentTimeMillis();
        System.err.println(this+": synchronize: TODO");
        return new ResourceSynchronizationResult(startTime, System.currentTimeMillis(), ResourceStatus.HEALTHY, null, null);
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

    private static String join(Iterable<String> strings) {
        StringBuilder sb = new StringBuilder();
        for(String str : strings) {
            if(sb.length()>0) sb.append(',');
            sb.append(str);
        }
        return sb.toString();
    }

    /**
     * First run csync2 -G GROUPS -cr /
     *  Must exit 0
     * Then run csync2 -G GROUPS -T LOCAL_NODE REMOTE_NODE
     *  Exit 0 means warning
     *  Exit 2 means everything is OK
     *  Other exit means error
     */
    @Override
    protected ResourceTestResult test(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        long startTime = System.currentTimeMillis();
        Csync2Resource resource = localResourceNode.getResource();
        String[] command = new String[] {
            localResourceNode.getExe(),
            "-G",
            join(resource.getGroups()),
            "-T",
            localResourceNode.getNode().getHostname().toString(),
            remoteResourceNode.getNode().getHostname().toString()
        };
        System.err.println(this+": test: TODO");
        return new ResourceTestResult(startTime, System.currentTimeMillis(), ResourceStatus.HEALTHY, null, null);
    }
}
