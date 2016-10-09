/*
 * ao-appcluster - Application-level clustering tools.
 * Copyright (C) 2011, 2015, 2016  AO Industries, Inc.
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
import com.aoindustries.util.ErrorPrinter;
import java.util.ArrayList;
import java.util.Collections;
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
	private ResourceSynchronizationMode synchronizeNowMode; // All access uses jobLock
	private ResourceSynchronizationResult lastResult; // All access uses jobLock

	protected CronResourceSynchronizer(RN localResourceNode, RN remoteResourceNode, Schedule synchronizeSchedule, Schedule testSchedule) {
		super(localResourceNode, remoteResourceNode);
		this.synchronizeSchedule = synchronizeSchedule;
		this.testSchedule = testSchedule;
		List<Schedule> combined = new ArrayList<>(2);
		combined.add(synchronizeSchedule);
		combined.add(testSchedule);
		this.combinedSchedule = new MultiSchedule(combined);
		this.state = ResourceSynchronizerState.STOPPED;
		this.stateMessage = null;
		this.synchronizeNowMode = null;
		this.lastResult = null;
	}

	/**
	 * Gets the synchronize schedule.
	 */
	public Schedule getSynchronizeSchedule() {
		return synchronizeSchedule;
	}

	/**
	 * Determines if the synchronizer can run now.
	 */
	public boolean canSynchronizeNow(ResourceSynchronizationMode mode) {
		synchronized(jobLock) {
			if(job==null) return false;
			if(state!=ResourceSynchronizerState.SLEEPING) return false;
		}
		R resource = localResourceNode.getResource();
		ResourceDnsResult resourceDnsResult = resource.getDnsMonitor().getLastResult();
		// Not allowed to synchronize while inconsistent
		if(resourceDnsResult.getResourceStatus()==ResourceStatus.INCONSISTENT) return false;
		Map<? extends Node,? extends ResourceNodeDnsResult> nodeResultMap = resourceDnsResult.getNodeResultMap();
		final ResourceNodeDnsResult localDnsResult = nodeResultMap.get(localResourceNode.getNode());
		final ResourceNodeDnsResult remoteDnsResult = nodeResultMap.get(remoteResourceNode.getNode());
		return canSynchronize(mode, localDnsResult, remoteDnsResult);
	}

	/**
	 * Convenience method for JavaBeans property.
	 *
	 * @see #canSynchronizeNow(com.aoindustries.appcluster.ResourceSynchronizationMode)
	 */
	public boolean getCanSynchronizeNow() {
		return canSynchronizeNow(ResourceSynchronizationMode.SYNCHRONIZE);
	}

	/**
	 * Convenience method for JavaBeans property.
	 *
	 * @see #canTestNow(com.aoindustries.appcluster.ResourceSynchronizationMode)
	 */
	public boolean getCanTestNow() {
		return canSynchronizeNow(ResourceSynchronizationMode.TEST_ONLY);
	}

	/**
	 * Schedules an immediate synchronization if the resource is enabled and
	 * sleeping.
	 */
	@Override
	public void synchronizeNow(ResourceSynchronizationMode mode) {
		synchronized(jobLock) {
			if(job!=null && state==ResourceSynchronizerState.SLEEPING) {
				synchronizeNowMode = mode;
				CronDaemon.runImmediately(job);
			}
		}
	}

	/**
	 * Gets the test schedule.
	 */
	public Schedule getTestSchedule() {
		return testSchedule;
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

	@Override
	public ResourceSynchronizationResult getLastResult() {
		synchronized(jobLock) {
			return lastResult;
		}
	}

	/**
	 * Saves the last result to be restored later.  Not yet implemented.
	 */
	private void saveLastResult(ResourceSynchronizationResult result) {
		assert Thread.holdsLock(jobLock);
		// Not yet implemented
	}

	/**
	 * Reads the last result that was saved.
	 */
	private ResourceSynchronizationResult loadLastResult() {
		assert Thread.holdsLock(jobLock);
		// Not yet implemented
		return null;
	}

	/**
	 * Sets the last result, firing listeners, too.
	 */
	private void setLastResult(ResourceSynchronizationResult newResult) {
		assert Thread.holdsLock(jobLock);
		ResourceSynchronizationResult oldResult = this.lastResult;
		this.lastResult = newResult;
		saveLastResult(newResult);

		// Notify listeners
		localResourceNode.getResource().getCluster().notifyResourceListenersOnSynchronizationResult(oldResult, newResult);
	}

	@Override
	protected void start() {
		synchronized(jobLock) {
			R resource = localResourceNode.getResource();
			if(!resource.getCluster().isEnabled()) {
				state = ResourceSynchronizerState.DISABLED;
				stateMessage = ApplicationResources.accessor.getMessage("CronResourceSynchronizer.start.clusterDisabled.stateMessage");
				synchronizeNowMode = null;
				lastResult = loadLastResult();
			} else if(!resource.isEnabled()) {
				state = ResourceSynchronizerState.DISABLED;
				stateMessage = ApplicationResources.accessor.getMessage("CronResourceSynchronizer.start.resourceDisabled.stateMessage");
				synchronizeNowMode = null;
				lastResult = loadLastResult();
			} else if(!localResourceNode.getNode().isEnabled()) {
				state = ResourceSynchronizerState.DISABLED;
				stateMessage = ApplicationResources.accessor.getMessage("CronResourceSynchronizer.start.localNodeDisabled.stateMessage");
				synchronizeNowMode = null;
				lastResult = loadLastResult();
			} else if(!remoteResourceNode.getNode().isEnabled()) {
				state = ResourceSynchronizerState.DISABLED;
				stateMessage = ApplicationResources.accessor.getMessage("CronResourceSynchronizer.start.remoteNodeDisabled.stateMessage");
				synchronizeNowMode = null;
				lastResult = loadLastResult();
			} else {
				if(job==null) {
					state = ResourceSynchronizerState.SLEEPING;
					stateMessage = null;
					synchronizeNowMode = null;
					lastResult = loadLastResult();

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
							final ResourceSynchronizationMode synchronizeNowMode;
							synchronized(jobLock) {
								if(job!=this) return;
								synchronizeNowMode = CronResourceSynchronizer.this.synchronizeNowMode;
								CronResourceSynchronizer.this.synchronizeNowMode = null;
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
										synchronizeNowMode == ResourceSynchronizationMode.SYNCHRONIZE
										|| (
											synchronizeNowMode == null
											&& synchronizeSchedule.isCronJobScheduled(minute, hour, dayOfMonth, month, dayOfWeek, year)
										)
									) && canSynchronize(ResourceSynchronizationMode.SYNCHRONIZE, localDnsResult, remoteDnsResult)
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
													return synchronize(ResourceSynchronizationMode.SYNCHRONIZE, localDnsResult, remoteDnsResult);
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
										result = new ResourceSynchronizationResult(
											localResourceNode,
											remoteResourceNode,
											ResourceSynchronizationMode.SYNCHRONIZE,
											Collections.singletonList(
												new ResourceSynchronizationResultStep(
													startTime,
													System.currentTimeMillis(),
													ResourceStatus.ERROR,
													"future.get",
													null,
													null,
													Collections.singletonList(ErrorPrinter.getStackTraces(err))
												)
											)
										);
									}
									synchronized(jobLock) {
										if(job!=this) return;
										state = ResourceSynchronizerState.SLEEPING;
										stateMessage = null;
										setLastResult(result);
									}
								} else if(
									(
										synchronizeNowMode == ResourceSynchronizationMode.TEST_ONLY
										|| (
											synchronizeNowMode == null
											&& testSchedule.isCronJobScheduled(minute, hour, dayOfMonth, month, dayOfWeek, year)
										)
									) && canSynchronize(ResourceSynchronizationMode.TEST_ONLY, localDnsResult, remoteDnsResult)
								) {
									// Perform test
									synchronized(jobLock) {
										if(job!=this) return;
										state = ResourceSynchronizerState.TESTING;
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
													return synchronize(ResourceSynchronizationMode.TEST_ONLY, localDnsResult, remoteDnsResult);
												} finally {
													currentThread.setPriority(oldThreadPriority);
												}
											}
										}
									);
									ResourceSynchronizationResult result;
									try {
										result = future.get(resource.getTestTimeout(), TimeUnit.SECONDS);
									} catch(Exception err) {
										result = new ResourceSynchronizationResult(
											localResourceNode,
											remoteResourceNode,
											ResourceSynchronizationMode.TEST_ONLY,
											Collections.singletonList(
												new ResourceSynchronizationResultStep(
													startTime,
													System.currentTimeMillis(),
													ResourceStatus.ERROR,
													"future.get",
													null,
													null,
													Collections.singletonList(ErrorPrinter.getStackTraces(err))
												)
											)
										);
									}
									synchronized(jobLock) {
										if(job!=this) return;
										state = ResourceSynchronizerState.SLEEPING;
										stateMessage = null;
										setLastResult(result);
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
				synchronizeNowMode = null;
				lastResult = null;
			}
		}
	}

	/**
	 * Checks if a resource may be synchronized given the current DNS state of the nodes.
	 */
	abstract protected boolean canSynchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult);

	/**
	 * Synchronizes (or tests) the resource.
	 */
	abstract protected ResourceSynchronizationResult synchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult);
}
