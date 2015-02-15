/*
 * ao-appcluster - Coordinates system components installed in master/slave replication.
 * Copyright (C) 2011, 2015  AO Industries, Inc.
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
     * Gets either an empty list or singleton list, containing str.
     */
    private static List<String> getList(String str) {
        if(str==null || str.length()==0) return Collections.emptyList();
        return Collections.singletonList(str);
    }

    /**
     * Builds a command string, hiding the full path of the first parameter.
     */
    private static String buildCommandString(String[] command) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<command.length; i++) {
            String arg = command[i];
            if(i==0) {
                int pos = arg.lastIndexOf('/');
                if(pos!=-1) arg = arg.substring(pos+1);
            } else {
                sb.append(' ');
            }
            sb.append(arg);
        }
        return sb.toString();
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
        final String exe = localResourceNode.getExe();
        final String localHostname = localResourceNode.getNode().getHostname().toString();
        final String remoteHostname = remoteResourceNode.getNode().getHostname().toString();
        final Csync2Resource resource = localResourceNode.getResource();
        final String groups = StringUtility.join(resource.getGroups(), ",");

        List<ResourceSynchronizationResultStep> steps = new ArrayList<>(2);

        // Step one: synchronize or scan
        {
            long startTime = System.currentTimeMillis();
            String[] command;
            switch(mode) {
                case SYNCHRONIZE :
                {
                    command = new String[] {exe, "-G", groups, "-P", remoteHostname, "-xv"};
                    break;
                }
                case TEST_ONLY :
                {
                    command = new String[] {exe, "-G", groups, "-cr", "/"};
                    break;
                }
                default :
                    throw new AssertionError("Unexpected mode: "+mode);
            }
            String commandString = buildCommandString(command);
            ResourceSynchronizationResultStep step;
            try {
                ProcessResult processResult = ProcessResult.exec(command);
                step = new ResourceSynchronizationResultStep(
                    startTime,
                    System.currentTimeMillis(),
                    processResult.getExitVal()==0 ? ResourceStatus.HEALTHY : ResourceStatus.ERROR,
                    commandString,
                    getList(processResult.getStdout()),
                    null,
                    getList(processResult.getStderr())
                );
            } catch(Exception exc) {
                step = new ResourceSynchronizationResultStep(
                    startTime,
                    System.currentTimeMillis(),
                    ResourceStatus.ERROR,
                    commandString,
                    null,
                    null,
                    Collections.singletonList(ErrorPrinter.getStackTraces(exc))
                );
            }
            steps.add(step);
        }

        // Step two: test
        {
            long startTime = System.currentTimeMillis();
            String[] command = {exe, "-G", groups, "-T", localHostname, remoteHostname};
            String commandString = buildCommandString(command);

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
                    getList(processResult.getStdout()),
                    exitVal==0 ? getList(processResult.getStderr()) : null,
                    exitVal!=0 ? getList(processResult.getStderr()) : null
                );
            } catch(Exception exc) {
                step = new ResourceSynchronizationResultStep(
                    startTime,
                    System.currentTimeMillis(),
                    ResourceStatus.ERROR,
                    commandString,
                    null,
                    null,
                    Collections.singletonList(ErrorPrinter.getStackTraces(exc))
                );
            }
            steps.add(step);
        }

        return new ResourceSynchronizationResult(localResourceNode, remoteResourceNode, mode, steps);
    }
}
