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
import com.aoindustries.appcluster.ResourceSynchronizationMode;
import com.aoindustries.appcluster.ResourceSynchronizationResult;
import com.aoindustries.appcluster.ResourceSynchronizationResultStep;
import com.aoindustries.cron.Schedule;
import com.aoindustries.lang.ProcessResult;
import com.aoindustries.util.ErrorPrinter;
import com.aoindustries.util.StringUtility;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
     * May synchronize and test from any master or slave to any master or slave.
     */
    @Override
    protected boolean canSynchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        NodeDnsStatus localDnsStatus = localDnsResult.getNodeStatus();
        NodeDnsStatus remoteDnsStatus = remoteDnsResult.getNodeStatus();
        switch(mode) {
            case SYNCHRONIZE :
            case TEST_ONLY :
                return
                    (
                        localDnsStatus==NodeDnsStatus.MASTER
                        || localDnsStatus==NodeDnsStatus.SLAVE
                    ) && (
                        remoteDnsStatus==NodeDnsStatus.MASTER
                        || remoteDnsStatus==NodeDnsStatus.SLAVE
                    )
                ;
            default :
                throw new AssertionError("Unexpected mode: "+mode);
        }
    }

    /**
     * <ol>
     *   <li>
     *     For synchronize:
     *       First run csync2 -G GROUPS -P REMOTE_NODE -xv
     *       Must exit 0
     *     For test:
     *       First run csync2 -G GROUPS -cr /
     *       Must exit 0
     *   </li>
     *   <li>
     *     Then run csync2 -G GROUPS -T LOCAL_NODE REMOTE_NODE
     *     Exit 0 means warning
     *     Exit 2 means everything is OK
     *     Other exit means error
     *   </li>
     * </ol>
     */
    @Override
    protected ResourceSynchronizationResult synchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
        final String localHostname = localResourceNode.getNode().getHostname().toString();
        final String remoteHostname = remoteResourceNode.getNode().getHostname().toString();
        final Csync2Resource resource = localResourceNode.getResource();
        final String groups = StringUtility.join(resource.getGroups(), ",");

        List<ResourceSynchronizationResultStep> steps = new ArrayList<ResourceSynchronizationResultStep>(2);

        // Step one: synchronize or scan
        {
            long startTime = System.currentTimeMillis();
            String[] command;
            switch(mode) {
                case SYNCHRONIZE :
                {
                    command = new String[] {
                        localResourceNode.getExe(),
                        "-G",
                        groups,
                        "-P",
                        remoteHostname,
                        "-xv"
                    };
                    break;
                }
                case TEST_ONLY :
                {
                    command = new String[] {
                        localResourceNode.getExe(),
                        "-G",
                        groups,
                        "-cr",
                        "/"
                    };
                    break;
                }
                default :
                    throw new AssertionError("Unexpected mode: "+mode);
            }
            String commandString = StringUtility.join(command, " ");
            ResourceSynchronizationResultStep step;
            try {
                ProcessResult processResult = ProcessResult.exec(command);
                step = new ResourceSynchronizationResultStep(
                    startTime,
                    System.currentTimeMillis(),
                    processResult.getExitVal()==0 ? ResourceStatus.HEALTHY : ResourceStatus.ERROR,
                    commandString,
                    Collections.singletonList(processResult.getStdout()),
                    Collections.singletonList(processResult.getStderr())
                );
            } catch(Exception exc) {
                step = new ResourceSynchronizationResultStep(
                    startTime,
                    System.currentTimeMillis(),
                    ResourceStatus.ERROR,
                    commandString,
                    null,
                    Collections.singletonList(ErrorPrinter.getStackTraces(exc))
                );
            }
            steps.add(step);
        }

        // Step two: test
        {
            long startTime = System.currentTimeMillis();
            String[] command = {
                localResourceNode.getExe(),
                "-G",
                groups,
                "-T",
                localHostname,
                remoteHostname
            };
            String commandString = StringUtility.join(command, " ");

            ResourceSynchronizationResultStep step;
            try {
                ProcessResult processResult = ProcessResult.exec(command);
                int exitVal = processResult.getExitVal();
                step = new ResourceSynchronizationResultStep(
                    startTime,
                    System.currentTimeMillis(),
                    exitVal==2 ? ResourceStatus.HEALTHY
                    : exitVal==0 ? ResourceStatus.WARNING
                    : ResourceStatus.ERROR,
                    commandString,
                    Collections.singletonList(processResult.getStdout()),
                    Collections.singletonList(processResult.getStderr())
                );
            } catch(Exception exc) {
                step = new ResourceSynchronizationResultStep(
                    startTime,
                    System.currentTimeMillis(),
                    ResourceStatus.ERROR,
                    commandString,
                    null,
                    Collections.singletonList(ErrorPrinter.getStackTraces(exc))
                );
            }
            steps.add(step);
        }

        return new ResourceSynchronizationResult(mode, steps);
    }
}
