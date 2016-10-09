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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.xbill.DNS.Name;

/**
 * One node within the cluster.
 *
 * @author  AO Industries, Inc.
 */
public class Node {

	private final AppCluster cluster;
	private final String id;
	private final boolean enabled;
	private final String display;
	private final Name hostname;
	private final String username;
	private final Set<? extends Nameserver> nameservers;

	Node(AppCluster cluster, NodeConfiguration nodeConfiguration) {
		this.cluster = cluster;
		this.id = nodeConfiguration.getId();
		this.enabled = cluster.isEnabled() && nodeConfiguration.isEnabled();
		this.display = nodeConfiguration.getDisplay();
		this.hostname = nodeConfiguration.getHostname();
		this.username = nodeConfiguration.getUsername();
		Set<? extends Name> configNameservers = nodeConfiguration.getNameservers();
		Set<Nameserver> newNameservers = new LinkedHashSet<>(configNameservers.size()*4/3+1);
		for(Name nameserver : configNameservers) newNameservers.add(new Nameserver(cluster, nameserver));
		this.nameservers = AoCollections.optimalUnmodifiableSet(newNameservers);
	}

	@Override
	public String toString() {
		return display;
	}

	@Override
	public boolean equals(Object o) {
		if(!(o instanceof Node)) return false;
		return id.equals(((Node)o).getId());
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	/**
	 * Gets the unique identifier for this node.
	 */
	public String getId() {
		return id;
	}

	/**
	 * Determines if both the cluster and this node are enabled.
	 */
	public boolean isEnabled() {
		return enabled;
	}

	/**
	 * Gets the display name of this node.
	 */
	public String getDisplay() {
		return display;
	}

	/**
	 * Gets the hostname of the machine that runs this node.
	 */
	public Name getHostname() {
		return hostname;
	}

	/**
	 * Gets the username the cluster must run as on the machine that runs this node.
	 * This must match the system property "user.name" for this JVM to be considered
	 * as "localNode".
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Gets the set of nameservers that are local to the machine running this node.
	 */
	public Set<? extends Nameserver> getNameservers() {
		return nameservers;
	}

	/**
	 * Gets the overall status of the this node based on enabled and all resourceNodes.
	 */
	public ResourceStatus getStatus() {
		ResourceStatus status = ResourceStatus.UNKNOWN;
		if(!enabled) status = AppCluster.max(status, ResourceStatus.DISABLED);
		for(Resource<?,?> resource : cluster.getResources()) {
			ResourceNodeDnsResult nodeDnsResult = resource.getDnsMonitor().getLastResult().getNodeResultMap().get(this);
			if(nodeDnsResult!=null) {
				status = AppCluster.max(status, nodeDnsResult.getNodeStatus().getResourceStatus());
				Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> nodeLookups = nodeDnsResult.getNodeRecordLookups();
				if(nodeLookups!=null) {
					for(Map<? extends Nameserver,? extends DnsLookupResult> lookups : nodeLookups.values()) {
						for(Nameserver nameserver : nameservers) status = AppCluster.max(status, lookups.get(nameserver).getStatus().getResourceStatus());
					}
				}
			}
			ResourceSynchronizer<?,?> synchronizer = resource.getSynchronizerMap().get(this);
			if(synchronizer!=null) status = AppCluster.max(status, synchronizer.getResultStatus());
		}
		return status;
	}
}
