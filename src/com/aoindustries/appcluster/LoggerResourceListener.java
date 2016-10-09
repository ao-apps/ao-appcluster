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

import com.aoindustries.util.AoCollections;
import com.aoindustries.util.StringUtility;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xbill.DNS.Name;

/**
 * Logs all changes of resource state to the logger.
 *
 * @author  AO Industries, Inc.
 */
public class LoggerResourceListener implements ResourceListener {

	private static final Logger logger = Logger.getLogger(LoggerResourceListener.class.getName());

	@Override
	public void onResourceDnsResult(ResourceDnsResult oldResult, ResourceDnsResult newResult) {
		final Resource<?,?> resource = newResult.getResource();
		final AppCluster cluster = resource.getCluster();

		// Log any changes, except continual changes to time
		if(logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.timeMillis", cluster, resource, newResult.endTime - newResult.startTime));
		}
		// Log any master DNS record change
		Level level;
		{
			Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> newMasterLookupResults = newResult.getMasterRecordLookups();
			Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> oldMasterLookupResults = oldResult.getMasterRecordLookups();
			if(newMasterLookupResults!=null) {
				for(Name masterRecord : resource.getMasterRecords()) {
					Map<? extends Nameserver,? extends DnsLookupResult> newMasterLookups = newMasterLookupResults.get(masterRecord);
					Map<? extends Nameserver,? extends DnsLookupResult> oldMasterLookups = oldMasterLookupResults==null ? null : oldMasterLookupResults.get(masterRecord);
					for(Nameserver enabledNameserver : resource.getEnabledNameservers()) {
						DnsLookupResult newDnsLookupResult = newMasterLookups.get(enabledNameserver);
						level = newDnsLookupResult.getStatus().getResourceStatus().getLogLevel();
						if(logger.isLoggable(level)) {
							DnsLookupResult oldDnsLookupResult = oldMasterLookups==null ? null : oldMasterLookups.get(enabledNameserver);
							SortedSet<String> newAddresses = newDnsLookupResult.getAddresses();
							SortedSet<String> oldAddresses = oldDnsLookupResult==null ? null : oldDnsLookupResult.getAddresses();
							if(oldAddresses==null) oldAddresses = AoCollections.emptySortedSet();
							if(!newAddresses.equals(oldAddresses)) {
								logger.log(
									level,
									ApplicationResources.accessor.getMessage(
										"LoggingResourceDnsMonitor.onResourceDnsResult.masterRecordLookupResultChanged",
										cluster,
										resource,
										masterRecord,
										enabledNameserver,
										oldAddresses==null ? "" : StringUtility.join(oldAddresses, ", "),
										StringUtility.join(newAddresses, ", ")
									)
								);
							}
							SortedSet<String> newStatusMessages = newDnsLookupResult.getStatusMessages();
							SortedSet<String> oldStatusMessages = oldDnsLookupResult==null ? null : oldDnsLookupResult.getStatusMessages();
							if(oldStatusMessages==null) oldStatusMessages = AoCollections.emptySortedSet();
							if(!newStatusMessages.equals(oldStatusMessages)) {
								for(String statusMessage : newStatusMessages) {
									logger.log(
										level,
										ApplicationResources.accessor.getMessage(
											"LoggingResourceDnsMonitor.onResourceDnsResult.masterRecord.statusMessage",
											cluster,
											resource,
											masterRecord,
											enabledNameserver,
											statusMessage
										)
									);
								}
							}
						}
					}
				}
			}
		}
		level = newResult.getMasterStatus().getResourceStatus().getLogLevel();
		if(logger.isLoggable(level)) {
			if(newResult.getMasterStatus()!=oldResult.getMasterStatus()) {
				logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.masterStatusChanged", cluster, resource, oldResult.getMasterStatus(), newResult.getMasterStatus()));
			}
			if(!newResult.getMasterStatusMessages().equals(oldResult.getMasterStatusMessages())) {
				for(String masterStatusMessage : newResult.getMasterStatusMessages()) {
					logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.masterStatusMessage", cluster, resource, masterStatusMessage));
				}
			}
		}
		for(ResourceNode<?,?> resourceNode : resource.getResourceNodes()) {
			Node node = resourceNode.getNode();
			ResourceNodeDnsResult newNodeResult = newResult.getNodeResultMap().get(node);
			ResourceNodeDnsResult oldNodeResult = oldResult.getNodeResultMap().get(node);
			// Log any node DNS record change
			{
				Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> newNodeLookupResults = newNodeResult.getNodeRecordLookups();
				Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> oldNodeLookupResults = oldNodeResult.getNodeRecordLookups();
				if(newNodeLookupResults!=null) {
					for(Name nodeRecord : resourceNode.getNodeRecords()) {
						Map<? extends Nameserver,? extends DnsLookupResult> newNodeLookups = newNodeLookupResults.get(nodeRecord);
						Map<? extends Nameserver,? extends DnsLookupResult> oldNodeLookups = oldNodeLookupResults==null ? null : oldNodeLookupResults.get(nodeRecord);
						for(Nameserver enabledNameserver : resource.getEnabledNameservers()) {
							DnsLookupResult newDnsLookupResult = newNodeLookups.get(enabledNameserver);
							level = newDnsLookupResult.getStatus().getResourceStatus().getLogLevel();
							if(logger.isLoggable(level)) {
								DnsLookupResult oldDnsLookupResult = oldNodeLookups==null ? null : oldNodeLookups.get(enabledNameserver);
								SortedSet<String> newAddresses = newDnsLookupResult.getAddresses();
								SortedSet<String> oldAddresses = oldDnsLookupResult==null ? null : oldDnsLookupResult.getAddresses();
								if(oldAddresses==null) oldAddresses = AoCollections.emptySortedSet();
								if(!newAddresses.equals(oldAddresses)) {
									logger.log(
										level,
										ApplicationResources.accessor.getMessage(
											"LoggingResourceDnsMonitor.onResourceDnsResult.nodeRecordLookupResultChanged",
											cluster,
											resource,
											node,
											nodeRecord,
											enabledNameserver,
											oldAddresses==null ? "" : StringUtility.join(oldAddresses, ", "),
											StringUtility.join(newAddresses, ", ")
										)
									);
								}
								SortedSet<String> newStatusMessages = newDnsLookupResult.getStatusMessages();
								SortedSet<String> oldStatusMessages = oldDnsLookupResult==null ? null : oldDnsLookupResult.getStatusMessages();
								if(oldStatusMessages==null) oldStatusMessages = AoCollections.emptySortedSet();
								if(!newStatusMessages.equals(oldStatusMessages)) {
									for(String statusMessage : newStatusMessages) {
										logger.log(
											level,
											ApplicationResources.accessor.getMessage(
												"LoggingResourceDnsMonitor.onResourceDnsResult.nodeRecord.statusMessage",
												cluster,
												resource,
												node,
												nodeRecord,
												enabledNameserver,
												statusMessage
											)
										);
									}
								}
							}
						}
					}
				}
			}
			NodeDnsStatus newNodeStatus = newNodeResult.getNodeStatus();
			level = newNodeStatus.getResourceStatus().getLogLevel();
			if(logger.isLoggable(level)) {
				NodeDnsStatus oldNodeStatus = oldNodeResult.getNodeStatus();
				if(newNodeStatus!=oldNodeStatus) {
					logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.nodeStatusChanged", cluster, resource, node, oldNodeStatus, newNodeStatus));
				}
				SortedSet<String> newNodeStatusMessages = newNodeResult.getNodeStatusMessages();
				SortedSet<String> oldNodeStatusMessages = oldNodeResult.getNodeStatusMessages();
				if(!newNodeStatusMessages.equals(oldNodeStatusMessages)) {
					for(String nodeStatusMessage : newNodeStatusMessages) {
						logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.nodeStatusMessage", cluster, resource, node, nodeStatusMessage));
					}
				}
			}
		}
	}

	/**
	 * Two results match if they have the same status, output, and errors for every step, in the same order.
	 * Note, this excludes mode, description, and times so that tests may be
	 * compared to synchronization passes.
	 */
	private static boolean matches(ResourceSynchronizationResult oldResult, ResourceSynchronizationResult newResult) {
		// Handle null
		if(oldResult==null) return newResult==null;
		if(newResult==null) return false; // oldResult==null;
		// Both non-null
		List<ResourceSynchronizationResultStep> oldSteps = oldResult.getSteps();
		List<ResourceSynchronizationResultStep> newSteps = newResult.getSteps();
		if(oldSteps.size()!=newSteps.size()) return false;
		for(int i=0, size=newSteps.size(); i<size; i++) {
			ResourceSynchronizationResultStep oldStep = oldSteps.get(i);
			ResourceSynchronizationResultStep newStep = newSteps.get(i);
			if(oldStep.getResourceStatus()!=newStep.getResourceStatus()) return false;
			if(!oldStep.getOutputs().equals(newStep.getOutputs())) return false;
			if(!oldStep.getErrors().equals(newStep.getErrors())) return false;
		}
		return true;
	}

	@Override
	public void onResourceSynchronizationResult(ResourceSynchronizationResult oldResult, ResourceSynchronizationResult newResult) {
		final ResourceNode<?,?> localResourceNode = newResult.getLocalResourceNode();
		final ResourceNode<?,?> remoteResourceNode = newResult.getRemoteResourceNode();
		final Resource<?,?> resource = localResourceNode.getResource();
		final AppCluster cluster = resource.getCluster();
		final Node localNode = localResourceNode.getNode();
		final Node remoteNode = remoteResourceNode.getNode();
		final ResourceSynchronizationMode mode = newResult.getMode();

		if(logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceSynchronizationResult.timeMillis", cluster, resource, localNode, remoteNode, mode, newResult.getEndTime().getTime() - newResult.getStartTime().getTime()));
		}

		ResourceStatus resourceStatus = newResult.getResourceStatus();
		Level level = resourceStatus.getLogLevel();
		if(logger.isLoggable(level) && !matches(oldResult, newResult)) {
			List<ResourceSynchronizationResultStep> steps = newResult.getSteps();
			for(int stepNum=1, size=steps.size(); stepNum<=size; stepNum++) {
				ResourceSynchronizationResultStep step = steps.get(stepNum-1);
				logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceSynchronizationResult.step.description", cluster, resource, localNode, remoteNode, mode, stepNum, step.getDescription()));
				logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceSynchronizationResult.step.startTime", cluster, resource, localNode, remoteNode, mode, stepNum, step.getStartTime()));
				logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceSynchronizationResult.step.endTime", cluster, resource, localNode, remoteNode, mode, stepNum, step.getEndTime()));
				logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceSynchronizationResult.step.status", cluster, resource, localNode, remoteNode, mode, stepNum, step.getResourceStatus()));
				for(String output: step.getOutputs()) {
					logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceSynchronizationResult.step.output", cluster, resource, localNode, remoteNode, mode, stepNum, output));
				}
				for(String error: step.getErrors()) {
					logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceSynchronizationResult.step.error", cluster, resource, localNode, remoteNode, mode, stepNum, error));
				}
			}
		}
	}
}
