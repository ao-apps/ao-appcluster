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

import java.util.Map;
import org.xbill.DNS.Name;

/**
 * A nameserver may be used by multiple nodes.
 *
 * @author  AO Industries, Inc.
 */
public class Nameserver {

    private final AppCluster cluster;
    private final Name hostname;

    Nameserver(AppCluster cluster, Name hostname) {
        this.cluster = cluster;
        this.hostname = hostname;
    }

    @Override
    public String toString() {
        return hostname.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof Nameserver)) return false;
        return hostname.equals(((Nameserver)o).hostname);
    }

    @Override
    public int hashCode() {
        return hostname.hashCode();
    }

    /**
     * Gets the cluster this nameserver is part of.
     */
    public AppCluster getCluster() {
        return cluster;
    }

    /**
     * Gets the hostname of this nameserver.
     */
    public Name getHostname() {
        return hostname;
    }

    /**
     * Gets the overall status of the this nameserver based on all resourceNodes that use this nameserver.
     */
    public ResourceStatus getStatus() {
        ResourceStatus status = ResourceStatus.UNKNOWN;
        for(Resource resource : cluster.getResources()) {
            ResourceDnsResult resourceDnsResult = resource.getDnsMonitor().getLastResult();
            Map<Name,Map<Nameserver,DnsLookupResult>> masterDnsLookups = resourceDnsResult.getMasterRecordLookups();
            if(masterDnsLookups!=null) {
                for(Map<Nameserver,DnsLookupResult> lookups : masterDnsLookups.values()) {
                    DnsLookupResult lookup = lookups.get(this);
                    if(lookup!=null) status = AppCluster.max(status, lookup.getStatus().getResourceStatus());
                }
            }

            for(ResourceNodeDnsResult nodeDnsResult : resourceDnsResult.getNodeResults().values()) {
                Map<Name,Map<Nameserver,DnsLookupResult>> nodeDnsLookups = nodeDnsResult.getNodeRecordLookups();
                if(nodeDnsLookups!=null) {
                    for(Map<Nameserver,DnsLookupResult> lookups : nodeDnsLookups.values()) {
                        DnsLookupResult lookup = lookups.get(this);
                        if(lookup!=null) status = AppCluster.max(status, lookup.getStatus().getResourceStatus());
                    }
                }
            }
        }
        return status;
    }
}
