/*
 * ao-appcluster - Application-level clustering tools.
 * Copyright (C) 2011, 2016  AO Industries, Inc.
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

import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import org.xbill.DNS.Name;

/**
 * Contains the results of one DNS monitoring pass for a single enabled node.
 *
 * @author  AO Industries, Inc.
 */
public class ResourceNodeDnsResult {

	private final ResourceNode<?,?> resourceNode;
	private final Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> nodeRecordLookups;
	private final NodeDnsStatus nodeStatus;
	private final SortedSet<String> nodeStatusMessages;

	ResourceNodeDnsResult(
		ResourceNode<?,?> resourceNode,
		Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> nodeRecordLookups,
		NodeDnsStatus nodeStatus,
		Collection<String> nodeStatusMessages
	) {
		this.resourceNode = resourceNode;
		this.nodeRecordLookups = nodeRecordLookups==null ? null : ResourceDnsResult.getUnmodifiableDnsLookupResults(nodeRecordLookups, resourceNode.getNodeRecords(), resourceNode.getResource().getEnabledNameservers());
		this.nodeStatus = nodeStatus;
		this.nodeStatusMessages = ResourceDnsResult.getUnmodifiableSortedSet(nodeStatusMessages, ResourceDnsResult.defaultLocaleCollator);
	}

	public ResourceNode<?,?> getResourceNode() {
		return resourceNode;
	}

	/**
	 * Gets the mapping of all nodeRecord DNS lookups in the form nodeRecord->enabledNameserver->result.
	 * If no lookups have been performed, such as during STOPPED or UNKNOWN state, returns <code>null</code>.
	 * Otherwise, it contains an entry for every nodeRecord querying every enabled nameserver.
	 */
	public Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> getNodeRecordLookups() {
		return nodeRecordLookups;
	}

	/**
	 * Gets the status of the node.
	 */
	public NodeDnsStatus getNodeStatus() {
		return nodeStatus;
	}

	/**
	 * Gets the node status messages.
	 * If no message, returns an empty set.
	 */
	public SortedSet<String> getNodeStatusMessages() {
		return nodeStatusMessages;
	}
}
