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
package com.aoindustries.appcluster;

import com.aoindustries.cron.CronDaemon;
import com.aoindustries.cron.CronJob;
import com.aoindustries.cron.CronJobScheduleMode;
import com.aoindustries.cron.MultiSchedule;
import com.aoindustries.cron.Schedule;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * <p>
 * Synchronizes resource based on cron-like schedules.
 * If the local node is stopped, the synchronizer is stopped.  If the
 * locale node or the remote node is disabled, the synchronized is disabled.
 * Otherwise, the synchronizer will operate on a cron-like scheduled basis for
 * testing and synchronizing.
 * </p>
 * <p>
 * To support masters synchronizing while slaves only test, if a test and a
 * synchronization are scheduled at the same moment, the synchronization is
 * performed if possible, and the test is only performed if the synchronization
 * is not possible.  Thus, a synchronization also counts as a test and should
 * perform at least as thorough of a check as a test would perform.
 * </p>
 * <p>
 * If a synchronization or test is missed, no catch-up is performed.  This is
 * true for both when the node is down or when a previous test/synchronization
 * took too long, overlapping the current schedule.
 * </p>
 *
 * @author  AO Industries, Inc.
 */
abstract public class CronResourceSynchronizer<R extends CronResource<R,RN>,RN extends CronResourceNode<R,RN>> extends ResourceSynchronizer<R,RN> {

    private static final Logger logger = Logger.getLogger(CronResourceSynchronizer.class.getName());

    private static final int THREAD_PRIORITY = Thread.NORM_PRIORITY - 2;

    private final Schedule synchronizeSchedule;
    private final Schedule testSchedule;
    private final Schedule combinedSchedule;
    
    private final Object jobLock = new Object();
    private CronJob job; // All access uses jobLock
    private ResourceSynchronizerState state; // All access uses jobLock
    private String stateMessage; // All access uses jobLock
    private boolean synchronizeNow; // All access uses jobLock
    private ResourceSynchronizationResult lastSynchronizationResult; // All access uses jobLock
    private boolean testNow; // All access uses jobLock
    private ResourceTestResult lastTestResult; // All access uses jobLock

    protected CronResourceSynchronizer(RN localResourceNode, RN remoteResourceNode, Schedule synchronizeSchedule, Schedule testSchedule) {
        super(localResourceNode, remoteResourceNode);
        this.synchronizeSchedule = synchronizeSchedule;
        this.testSchedule = testSchedule;
        List<Schedule> combined = new ArrayList<Schedule>(2);
        combined.add(synchronizeSchedule);
        combined.add(testSchedule);
        this.combinedSchedule = new MultiSchedule(combined);
        this.state = ResourceSynchronizerState.STOPPED;
        this.stateMessage = null;
        this.synchronizeNow = false;
        this.lastSynchronizationResult = null;
        this.testNow = false;
        this.lastTestResult = null;
    }

    @Override
    public ResourceSynchronizerState getState() {
        synchronized(jobLock) {
            return state;
        }
    }

    @Override
    public String getStateMessage() {
        synchronized(jobLock) {
            return stateMessage;
        }
    }

    /**
     * Schedules an immediate synchronization if the resource is enabled and
     * sleeping.
     */
    @Override
    public void synchronizeNow() {
        synchronized(jobLock) {
            if(job!=null && state==ResourceSynchronizerState.SLEEPING) {
                synchronizeNow = true;
                CronDaemon.runImmediately(job);
            }
        }
    }

    @Override
    public ResourceSynchronizationResult getLastSynchronizationResult() {
        synchronized(jobLock) {
            return lastSynchronizationResult;
        }
    }

    /**
     * Schedules an immediate test if the resource is enabled and
     * sleeping.
     */
    @Override
    public void testNow() {
        synchronized(jobLock) {
            if(job!=null && state==ResourceSynchronizerState.SLEEPING) {
                testNow = true;
                CronDaemon.runImmediately(job);
            }
        }
    }

    @Override
    public ResourceTestResult getLastTestResult() {
        synchronized(jobLock) {
            return lastTestResult;
        }
    }

    @Override
    protected void start() {
        synchronized(jobLock) {
            R resource = localResourceNode.getResource();
            if(!resource.getCluster().isEnabled()) {
                state = ResourceSynchronizerState.DISABLED;
                stateMessage = ApplicationResources.accessor.getMessage("CronResourceSynchronizer.start.clusterDisabled.stateMessage");
                synchronizeNow = false;
                lastSynchronizationResult = null; // TODO: Restore from persistence mechanism
                testNow = false;
                lastTestResult = null; // TODO: Restore from persistence mechanism
            } else if(!resource.isEnabled()) {
                state = ResourceSynchronizerState.DISABLED;
                stateMessage = ApplicationResources.accessor.getMessage("CronResourceSynchronizer.start.resourceDisabled.stateMessage");
                synchronizeNow = false;
                lastSynchronizationResult = null; // TODO: Restore from persistence mechanism
                testNow = false;
                lastTestResult = null; // TODO: Restore from persistence mechanism
            } else if(!localResourceNode.getNode().isEnabled()) {
                state = ResourceSynchronizerState.DISABLED;
                stateMessage = ApplicationResources.accessor.getMessage("CronResourceSynchronizer.start.localNodeDisabled.stateMessage");
                synchronizeNow = false;
                lastSynchronizationResult = null; // TODO: Restore from persistence mechanism
                testNow = false;
                lastTestResult = null; // TODO: Restore from persistence mechanism
            } else if(!remoteResourceNode.getNode().isEnabled()) {
                state = ResourceSynchronizerState.DISABLED;
                stateMessage = ApplicationResources.accessor.getMessage("CronResourceSynchronizer.start.remoteNodeDisabled.stateMessage");
                synchronizeNow = false;
                lastSynchronizationResult = null; // TODO: Restore from persistence mechanism
                testNow = false;
                lastTestResult = null; // TODO: Restore from persistence mechanism
            } else {
                if(job==null) {
                    state = ResourceSynchronizerState.SLEEPING;
                    stateMessage = null;
                    synchronizeNow = false;
                    lastSynchronizationResult = null; // TODO: Restore from persistence mechanism
                    testNow = false;
                    lastTestResult = null; // TODO: Restore from persistence mechanism
                    job = new CronJob() {
                        @Override
                        public Schedule getCronJobSchedule() {
                            return combinedSchedule;
                        }

                        @Override
                        public CronJobScheduleMode getCronJobScheduleMode() {
                            return CronJobScheduleMode.SKIP;
                        }

                        @Override
                        public String getCronJobName() {
                            return CronResourceSynchronizer.this.toString();
                        }

                        @Override
                        public void runCronJob(int minute, int hour, int dayOfMonth, int month, int dayOfWeek, int year) {
                            boolean synchronizeNow;
                            boolean testNow;
                            synchronized(jobLock) {
                                if(job!=this) return;
                                synchronizeNow = CronResourceSynchronizer.this.synchronizeNow;
                                if(synchronizeNow) CronResourceSynchronizer.this.synchronizeNow = false;
                                testNow = CronResourceSynchronizer.this.testNow;
                                if(testNow) CronResourceSynchronizer.this.testNow = false;
                            }
                            // Do not perform any synchronization or testing on an inconsistent resource
                            R resource = localResourceNode.getResource();
                            ResourceDnsResult resourceDnsResult = resource.getDnsMonitor().getLastResult();
                            if(resourceDnsResult.getResourceStatus()!=ResourceStatus.INCONSISTENT) {
                                // Find the node status of both the local and remote nodes
                                Map<? extends Node,? extends ResourceNodeDnsResult> nodeResultMap = resourceDnsResult.getNodeResultMap();
                                final ResourceNodeDnsResult localDnsResult = nodeResultMap.get(localResourceNode.getNode());
                                final ResourceNodeDnsResult remoteDnsResult = nodeResultMap.get(remoteResourceNode.getNode());
                                if(
                                    (
                                        synchronizeNow
                                        || synchronizeSchedule.isCronJobScheduled(minute, hour, dayOfMonth, month, dayOfWeek, year)
                                    ) && canSynchronize(localDnsResult, remoteDnsResult)
                                ) {
                                    // Perform synchronization
                                    synchronized(jobLock) {
                                        if(job!=this) return;
                                        state = ResourceSynchronizerState.SYNCHRONIZING;
                                        stateMessage = null;
                                    }
                                    long startTime = System.currentTimeMillis();
                                    Future<ResourceSynchronizationResult> future = resource.getCluster().getExecutorService().submit(
                                        new Callable<ResourceSynchronizationResult>() {
                                            @Override
                                            public ResourceSynchronizationResult call() throws Exception {
                                                final Thread currentThread = Thread.currentThread();
                                                final int oldThreadPriority = currentThread.getPriority();
                                                try {
                                                    currentThread.setPriority(THREAD_PRIORITY);
                                                    return synchronize(localDnsResult, remoteDnsResult);
                                                } finally {
                                                    currentThread.setPriority(oldThreadPriority);
                                                }
                                            }
                                        }
                                    );
                                    ResourceSynchronizationResult result;
                                    try {
                                        result = future.get(resource.getSynchronizeTimeout(), TimeUnit.SECONDS);
                                    } catch(Exception err) {
                                        result = new ResourceSynchronizationResult(startTime, System.currentTimeMillis(), ResourceStatus.ERROR, null, err.toString());
                                    }
                                    System.err.println("DEBUG: Got synchronize result:");
                                    System.err.println("    status="+result.getResourceStatus());
                                    System.err.println("    output="+result.getOutput());
                                    System.err.println("    error="+result.getError());
                                    synchronized(jobLock) {
                                        if(job!=this) return;
                                        state = ResourceSynchronizerState.SLEEPING;
                                        stateMessage = null;
                                        lastSynchronizationResult = result; // TODO: Store to persistence mechanism
                                    }
                                } else if(
                                    (
                                        testNow
                                        || testSchedule.isCronJobScheduled(minute, hour, dayOfMonth, month, dayOfWeek, year)
                                    ) && canTest(localDnsResult, remoteDnsResult)
                                ) {
                                    // Perform test
                                    synchronized(jobLock) {
                                        if(job!=this) return;
                                        state = ResourceSynchronizerState.TESTING;
                                        stateMessage = null;
                                        testNow = false;
                                    }
                                    long startTime = System.currentTimeMillis();
                                    Future<ResourceTestResult> future = resource.getCluster().getExecutorService().submit(
                                        new Callable<ResourceTestResult>() {
                                            @Override
                                            public ResourceTestResult call() throws Exception {
                                                final Thread currentThread = Thread.currentThread();
                                                final int oldThreadPriority = currentThread.getPriority();
                                                try {
                                                    currentThread.setPriority(THREAD_PRIORITY);
                                                    return test(localDnsResult, remoteDnsResult);
                                                } finally {
                                                    currentThread.setPriority(oldThreadPriority);
                                                }
                                            }
                                        }
                                    );
                                    ResourceTestResult result;
                                    try {
                                        result = future.get(resource.getTestTimeout(), TimeUnit.SECONDS);
                                    } catch(Exception err) {
                                        result = new ResourceTestResult(startTime, System.currentTimeMillis(), ResourceStatus.ERROR, null, err.toString());
                                    }
                                    System.err.println("DEBUG: Got test result:");
                                    System.err.println("    status="+result.getResourceStatus());
                                    System.err.println("    output="+result.getOutput());
                                    System.err.println("    error="+result.getError());
                                    synchronized(jobLock) {
                                        if(job!=this) return;
                                        state = ResourceSynchronizerState.SLEEPING;
                                        stateMessage = null;
                                        lastTestResult = result; // TODO: Store to persistence mechanism
                                    }
                                }
                            }
                        }

                        @Override
                        public int getCronJobThreadPriority() {
                            return THREAD_PRIORITY;
                        }
                    };
                    CronDaemon.addCronJob(job, logger);
                }
            }
        }
    }

    @Override
    protected void stop() {
        synchronized(jobLock) {
            if(job!=null) {
                CronDaemon.removeCronJob(job);
                job = null;
                state = ResourceSynchronizerState.STOPPED;
                stateMessage = null;
                synchronizeNow = false;
                lastSynchronizationResult = null;
                testNow = false;
                lastTestResult = null;
            }
        }
    }

    /**
     * Checks if a resource may be synchronized given the current DNS state of the nodes.
     */
    abstract protected boolean canSynchronize(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult);

    /**
     * Synchronizes the resource.
     */
    abstract protected ResourceSynchronizationResult synchronize(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult);

    /**
     * Checks if a resource may be tested given the current DNS state of the nodes.
     */
    abstract protected boolean canTest(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult);

    /**
     * Tests the resource.
     */
    abstract protected ResourceTestResult test(ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult);
}
