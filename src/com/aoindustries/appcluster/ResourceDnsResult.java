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
import java.sql.Timestamp;
import java.text.Collator;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.xbill.DNS.Name;

/**
 * Contains the results of one DNS monitoring pass.
 *
 * @author  AO Industries, Inc.
 */
public class ResourceDnsResult implements ResourceResult {

	public static final int WARNING_SECONDS = 10 + (ResourceDnsMonitor.DNS_CHECK_INTERVAL + ResourceDnsMonitor.DNS_ATTEMPTS * ResourceDnsMonitor.DNS_CHECK_TIMEOUT) / 1000;
	public static final int ERROR_SECONDS = WARNING_SECONDS + (ResourceDnsMonitor.DNS_CHECK_INTERVAL / 1000);

	static final Comparator<Object> defaultLocaleCollator = Collator.getInstance();

	static SortedSet<String> getUnmodifiableSortedSet(Collection<String> collection, Comparator<Object> collator) {
		if(collection==null || collection.isEmpty()) return AoCollections.emptySortedSet();
		if(collection.size()==1) return AoCollections.singletonSortedSet(collection.iterator().next());
		SortedSet<String> sortedSet = new TreeSet<>(collator);
		sortedSet.addAll(collection);
		return Collections.unmodifiableSortedSet(sortedSet);
	}

	static SortedSet<String> getUnmodifiableSortedSet(String[] array, Comparator<Object> collator) {
		if(array==null || array.length==0) return AoCollections.emptySortedSet();
		if(array.length==1) return AoCollections.singletonSortedSet(array[0]);
		SortedSet<String> sortedSet = new TreeSet<>(collator);
		sortedSet.addAll(Arrays.asList(array));
		return Collections.unmodifiableSortedSet(sortedSet);
	}

	/**
	 * Makes sure that every dnsRecord has a lookup for every nameserver.
	 * Also orders the maps by the dnsRecords and then nameservers.
	 * Returns a fully unmodifiable map.
	 *
	 * @exception  IllegalArgumentException  if any dnsRecord->nameserver result is missing.
	 */
	static Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> getUnmodifiableDnsLookupResults(Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> dnsRecordLookups, Set<? extends Name> dnsRecords, Set<? extends Nameserver> nameservers) throws IllegalArgumentException {
		Map<Name,Map<? extends Nameserver,? extends DnsLookupResult>> newDnsRecordLookups = new LinkedHashMap<>(dnsRecords.size()*4/3+1);
		for(Name dnsRecord : dnsRecords) {
			Map<? extends Nameserver,? extends DnsLookupResult> dnsLookupResults = dnsRecordLookups.get(dnsRecord);
			if(dnsLookupResults==null) throw new IllegalArgumentException("Missing DNS record " + dnsRecord);
			Map<Nameserver,DnsLookupResult> newDnsLookupResults = new LinkedHashMap<>(nameservers.size()*4/3+1);
			for(Nameserver nameserver : nameservers) {
				DnsLookupResult dnsLookupResult = dnsLookupResults.get(nameserver);
				if(dnsLookupResult==null) throw new IllegalArgumentException("Missing DNS lookup result " + dnsLookupResult);
				newDnsLookupResults.put(nameserver, dnsLookupResult);
			}
			newDnsRecordLookups.put(dnsRecord, AoCollections.optimalUnmodifiableMap(newDnsLookupResults));
		}
		return AoCollections.optimalUnmodifiableMap(newDnsRecordLookups);
	}

	private final Resource<?,?> resource;
	final long startTime;
	final long endTime;
	private final Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> masterRecordLookups;
	private final MasterDnsStatus masterStatus;
	private final SortedSet<String> masterStatusMessages;
	private final Map<? extends Node,? extends ResourceNodeDnsResult> nodeResults;

	ResourceDnsResult(
		Resource<?,?> resource,
		long startTime,
		long endTime,
		Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> masterRecordLookups,
		MasterDnsStatus masterStatus,
		Collection<String> masterStatusMessages,
		Map<? extends Node,? extends ResourceNodeDnsResult> nodeResults
	) {
		this.startTime = startTime;
		this.endTime = endTime;
		this.resource = resource;
		this.masterRecordLookups = masterRecordLookups==null ? null : getUnmodifiableDnsLookupResults(masterRecordLookups, resource.getMasterRecords(), resource.getEnabledNameservers());
		this.masterStatus = masterStatus;
		this.masterStatusMessages = getUnmodifiableSortedSet(masterStatusMessages, defaultLocaleCollator);
		Set<? extends ResourceNode<?,?>> resourceNodes = resource.getResourceNodes();
		Map<Node,ResourceNodeDnsResult> newNodeResults = new LinkedHashMap<>(resourceNodes.size()*4/3+1);
		for(ResourceNode<?,?> resourceNode : resourceNodes) {
			Node node = resourceNode.getNode();
			ResourceNodeDnsResult nodeResult = nodeResults.get(node);
			if(nodeResult==null) throw new IllegalArgumentException("Missing node " + node);
			newNodeResults.put(node, nodeResult);
		}
		this.nodeResults = AoCollections.optimalUnmodifiableMap(newNodeResults);
	}

	public Resource<?,?> getResource() {
		return resource;
	}

	@Override
	public Timestamp getStartTime() {
		return new Timestamp(startTime);
	}

	@Override
	public Timestamp getEndTime() {
		return new Timestamp(endTime);
	}

	/**
	 * Gets the number of seconds since this result had started or <code>null</code>
	 * if not running.
	 */
	public Long getSecondsSince() {
		if(!resource.getCluster().isRunning()) return null;
		if(!resource.isEnabled()) return null;
		return (System.currentTimeMillis() - startTime) / 1000;
	}

	/**
	 * Matches the rules for resource status.
	 *
	 * @see #getResourceStatus()
	 */
	public ResourceStatus getSecondsSinceStatus() {
		if(!resource.getCluster().isRunning()) return ResourceStatus.STOPPED;
		if(!resource.isEnabled()) return ResourceStatus.DISABLED;
		// Time since last result
		Long secondsSince = getSecondsSince();
		if(secondsSince==null) return ResourceStatus.UNKNOWN;
		// Error if result more than ERROR_SECONDS seconds ago
		if(secondsSince<-ERROR_SECONDS || secondsSince>ERROR_SECONDS) return ResourceStatus.ERROR;
		// Warning if result more than WARNING_SECONDS seconds ago
		if(secondsSince<-WARNING_SECONDS || secondsSince>WARNING_SECONDS) return ResourceStatus.WARNING;
		return ResourceStatus.HEALTHY;
	}

	/**
	 * Gets the mapping of all masterRecord DNS lookups in the form masterRecord->enabledNameserver->result.
	 * If no lookups have been performed, such as during STOPPED or UNKNOWN state, returns <code>null</code>.
	 * Otherwise, it contains an entry for every masterRecord querying every enabled nameserver.
	 */
	public Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> getMasterRecordLookups() {
		return masterRecordLookups;
	}

	/**
	 * Gets the status of the master records.
	 */
	public MasterDnsStatus getMasterStatus() {
		return masterStatus;
	}

	/**
	 * Gets the master status messages.
	 * If no message, returns an empty set.
	 */
	public SortedSet<String> getMasterStatusMessages() {
		return masterStatusMessages;
	}

	/**
	 * Gets the result of each node.
	 * This has an entry for every node in this resource.
	 */
	public Map<? extends Node,? extends ResourceNodeDnsResult> getNodeResultMap() {
		return nodeResults;
	}

	/**
	 * Gets the result of each node.
	 * This has an entry for every node in this resource.
	 */
	public Collection<? extends ResourceNodeDnsResult> getNodeResults() {
		return nodeResults.values();
	}

	/**
	 * Gets the ResourceStatus this result will cause.
	 */
	@Override
	public ResourceStatus getResourceStatus() {
		ResourceStatus status = ResourceStatus.UNKNOWN;

		// Check time since
		ResourceStatus secondsSinceStatus = getSecondsSinceStatus();
		if(secondsSinceStatus!=ResourceStatus.HEALTHY) status = AppCluster.max(status, secondsSinceStatus);

		// Master records
		status = AppCluster.max(status, getMasterStatus().getResourceStatus());
		if(masterRecordLookups!=null) {
			for(Map<? extends Nameserver,? extends DnsLookupResult> lookups : masterRecordLookups.values()) {
				for(DnsLookupResult lookup : lookups.values()) status = AppCluster.max(status, lookup.getStatus().getResourceStatus());
			}
		}

		// Node records
		for(ResourceNodeDnsResult nodeDnsResult : getNodeResultMap().values()) {
			status = AppCluster.max(status, nodeDnsResult.getNodeStatus().getResourceStatus());
			Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> nodeLookups = nodeDnsResult.getNodeRecordLookups();
			if(nodeLookups!=null) {
				for(Map<? extends Nameserver,? extends DnsLookupResult> lookups : nodeLookups.values()) {
					for(DnsLookupResult lookup : lookups.values()) status = AppCluster.max(status, lookup.getStatus().getResourceStatus());
				}
			}
		}

		return status;
	}
}
