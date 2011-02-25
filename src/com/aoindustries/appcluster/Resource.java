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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.xbill.DNS.Name;

/**
 * Monitors the status of a resource by monitoring its role based on DNS entries
 * and synchronizing the resource on an as-needed and/or scheduled basis.
 *
 * @see  ResourceDnsMonitor
 * 
 * @author  AO Industries, Inc.
 */
abstract public class Resource<R extends Resource<R,RN>,RN extends ResourceNode<R,RN>> {

    // private static final Logger logger = Logger.getLogger(Resource.class.getName());

    private final AppCluster cluster;
    private final String id;
    private final boolean enabled;
    private final String display;
    private final boolean allowMultiMaster;
    private final Set<Name> masterRecords;
    private final int masterRecordsTtl;
    private final Map<Node,RN> resourceNodes;
    private final Set<Name> enabledNameservers;

    private final ResourceDnsMonitor dnsMonitor;

    Resource(AppCluster cluster, AppClusterConfiguration.ResourceConfiguration resourceConfiguration, Map<Node,RN> resourceNodes) {
        @SuppressWarnings("unchecked")
        R rThis = (R)this;
        for(RN resourceNode : resourceNodes.values()) resourceNode.init(rThis);
        this.cluster = cluster;
        this.id = resourceConfiguration.getId();
        this.enabled = cluster.isEnabled() && resourceConfiguration.isEnabled();
        this.display = resourceConfiguration.getDisplay();
        this.allowMultiMaster = resourceConfiguration.getAllowMultiMaster();
        this.masterRecords = Collections.unmodifiableSet(new LinkedHashSet<Name>(resourceConfiguration.getMasterRecords()));
        this.masterRecordsTtl = resourceConfiguration.getMasterRecordsTtl();
        this.resourceNodes = resourceNodes;
        final Set<Name> newEnabledNameservers = new LinkedHashSet<Name>();
        for(Map.Entry<Node,RN> entry : resourceNodes.entrySet()) {
            Node node = entry.getKey();
            if(node.isEnabled()) newEnabledNameservers.addAll(node.getNameservers());
        }
        this.enabledNameservers = Collections.unmodifiableSet(newEnabledNameservers);

        this.dnsMonitor = new ResourceDnsMonitor(this);
    }

    @Override
    public String toString() {
        return display;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof Resource)) return false;
        return id.equals(((Resource)o).getId());
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    /**
     * Gets the cluster this resource is part of.
     */
    public AppCluster getCluster() {
        return cluster;
    }

    /**
     * The unique ID of this resource.
     */
    public String getId() {
        return id;
    }

    /**
     * Determines if both the cluster and this resource are enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Gets the display name of this resource.
     */
    public String getDisplay() {
        return display;
    }

    /**
     * Gets if this resource allows multiple master servers.
     */
    public boolean getAllowMultiMaster() {
        return allowMultiMaster;
    }

    /**
     * Gets the set of master records that must all by the same.
     * The master node is determined by matching these records against
     * the resource node configuration's node records.
     */
    public Set<Name> getMasterRecords() {
        return masterRecords;
    }

    /**
     * Gets the expected TTL value for the master record.
     */
    public int getMasterRecordTtl() {
        return masterRecordsTtl;
    }

    /**
     * Gets the set of all nameservers used by all enabled nodes.
     */
    public Set<Name> getEnabledNameservers() {
        return enabledNameservers;
    }

    /**
     * Gets the DNS monitor for this resource.
     */
    public ResourceDnsMonitor getDnsMonitor() {
        return dnsMonitor;
    }

    public Map<Node,RN> getResourceNodes() {
        return resourceNodes;
    }

    /**
     * Gets the status of this resource based on the last monitoring results.
     */
    public ResourceStatus getStatus() {
        if(!cluster.isRunning()) return ResourceStatus.STOPPED;
        if(!isEnabled()) return ResourceStatus.DISABLED;
        boolean hasUnknown = false;
        boolean hasWarning = false;
        boolean hasError = false;
        boolean hasInconsistent = false;
        ResourceDnsResult resourceDnsResult = getDnsMonitor().getLastResult();
        // TODO
        if(hasInconsistent) return ResourceStatus.INCONSISTENT;
        if(hasError) return ResourceStatus.ERROR;
        if(hasWarning) return ResourceStatus.WARNING;
        if(hasUnknown) return ResourceStatus.UNKNOWN;
        return ResourceStatus.HEALTHY;
    }
}
