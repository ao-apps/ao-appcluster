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

import java.text.Collator;
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
public class ResourceDnsResult {

    static final Comparator<Object> defaultLocaleCollator = Collator.getInstance();

    static SortedSet<String> getUnmodifiableSortedSet(Collection<String> collection, Comparator<Object> collator) {
        if(collection==null || collection.isEmpty()) return com.aoindustries.util.Collections.emptySortedSet();
        if(collection.size()==1) return com.aoindustries.util.Collections.singletonSortedSet(collection.iterator().next());
        SortedSet<String> sortedSet = new TreeSet<String>(collator);
        sortedSet.addAll(collection);
        return Collections.unmodifiableSortedSet(sortedSet);
    }

    static SortedSet<String> getUnmodifiableSortedSet(String[] array, Comparator<Object> collator) {
        if(array==null || array.length==0) return com.aoindustries.util.Collections.emptySortedSet();
        if(array.length==1) return com.aoindustries.util.Collections.singletonSortedSet(array[0]);
        SortedSet<String> sortedSet = new TreeSet<String>(collator);
        for(String elem : array) sortedSet.add(elem);
        return Collections.unmodifiableSortedSet(sortedSet);
    }

    /**
     * Makes sure that every dnsRecord has a lookup for every nameserver.
     * Also orders the maps by the dnsRecords and then nameservers.
     * Returns a fully unmodifiable map.
     *
     * @exception  IllegalArgumentException  if any dnsRecord->nameserver result is missing.
     */
    static Map<Name,Map<Nameserver,DnsLookupResult>> getUnmodifiableDnsLookupResults(Map<Name,Map<Nameserver,DnsLookupResult>> dnsRecordLookups, Set<Name> dnsRecords, Set<Nameserver> nameservers) throws IllegalArgumentException {
        Map<Name,Map<Nameserver,DnsLookupResult>> newDnsRecordLookups = new LinkedHashMap<Name,Map<Nameserver,DnsLookupResult>>(dnsRecords.size()*4/3+1);
        for(Name dnsRecord : dnsRecords) {
            Map<Nameserver,DnsLookupResult> dnsLookupResults = dnsRecordLookups.get(dnsRecord);
            if(dnsLookupResults==null) throw new IllegalArgumentException("Missing DNS record " + dnsRecord);
            Map<Nameserver,DnsLookupResult> newDnsLookupResults = new LinkedHashMap<Nameserver,DnsLookupResult>(nameservers.size()*4/3+1);
            for(Nameserver nameserver : nameservers) {
                DnsLookupResult dnsLookupResult = dnsLookupResults.get(nameserver);
                if(dnsLookupResult==null) throw new IllegalArgumentException("Missing DNS lookup result " + dnsLookupResult);
                newDnsLookupResults.put(nameserver, dnsLookupResult);
            }
            newDnsRecordLookups.put(dnsRecord, Collections.unmodifiableMap(newDnsLookupResults));
        }
        return Collections.unmodifiableMap(newDnsRecordLookups);
    }

    private final Resource<?,?> resource;
    private final long startTime;
    private final long endTime;
    private final Map<Name,Map<Nameserver,DnsLookupResult>> masterRecordLookups;
    private final MasterDnsStatus masterStatus;
    private final SortedSet<String> masterStatusMessages;
    private final Map<Node,ResourceNodeDnsResult> nodeResults;

    ResourceDnsResult(
        Resource<?,?> resource,
        long startTime,
        long endTime,
        Map<Name,Map<Nameserver,DnsLookupResult>> masterRecordLookups,
        MasterDnsStatus masterStatus,
        Collection<String> masterStatusMessages,
        Map<Node,ResourceNodeDnsResult> nodeResults
    ) {
        this.resource = resource;
        this.startTime = startTime;
        this.endTime = endTime;
        this.masterRecordLookups = masterRecordLookups==null ? null : getUnmodifiableDnsLookupResults(masterRecordLookups, resource.getMasterRecords(), resource.getEnabledNameservers());
        this.masterStatus = masterStatus;
        this.masterStatusMessages = getUnmodifiableSortedSet(masterStatusMessages, defaultLocaleCollator);
        Set<? extends ResourceNode<?,?>> resourceNodes = resource.getResourceNodes();
        Map<Node,ResourceNodeDnsResult> newNodeResults = new LinkedHashMap<Node,ResourceNodeDnsResult>(resourceNodes.size()*4/3+1);
        for(ResourceNode<?,?> resourceNode : resourceNodes) {
            Node node = resourceNode.getNode();
            ResourceNodeDnsResult nodeResult = nodeResults.get(node);
            if(nodeResult==null) throw new IllegalArgumentException("Missing node " + node);
            newNodeResults.put(node, nodeResult);
        }
        this.nodeResults = Collections.unmodifiableMap(newNodeResults);
    }

    public Resource<?,?> getResource() {
        return resource;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    /**
     * Gets the mapping of all masterRecord DNS lookups in the form masterRecord->enabledNameserver->result.
     * If no lookups have been performed, such as during STOPPED or UNKNOWN state, returns <code>null</code>.
     * Otherwise, it contains an entry for every masterRecord querying every enabled nameserver.
     */
    public Map<Name,Map<Nameserver,DnsLookupResult>> getMasterRecordLookups() {
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
    public Map<Node,ResourceNodeDnsResult> getNodeResults() {
        return nodeResults;
    }

    /**
     * Gets the ResourceStatus this result will cause.
     */
    public ResourceStatus getResourceStatus() {
        ResourceStatus status = ResourceStatus.UNKNOWN;

        // Time since last result
        long timeSince = System.currentTimeMillis()-getEndTime();
        // Error if result more than five minutes ago
        if(timeSince>300000) status = AppCluster.max(status, ResourceStatus.ERROR);
        // Warning if result more than one minute ago
        else if(timeSince>60000) status = AppCluster.max(status, ResourceStatus.WARNING);

        // Master records
        status = AppCluster.max(status, getMasterStatus().getResourceStatus());
        if(masterRecordLookups!=null) {
            for(Map<Nameserver,DnsLookupResult> lookups : masterRecordLookups.values()) {
                for(DnsLookupResult lookup : lookups.values()) status = AppCluster.max(status, lookup.getStatus().getResourceStatus());
            }
        }

        // Node records
        for(ResourceNodeDnsResult nodeDnsResult : getNodeResults().values()) {
            status = AppCluster.max(status, nodeDnsResult.getNodeStatus().getResourceStatus());
            Map<Name,Map<Nameserver,DnsLookupResult>> nodeLookups = nodeDnsResult.getNodeRecordLookups();
            if(nodeLookups!=null) {
                for(Map<Nameserver,DnsLookupResult> lookups : nodeLookups.values()) {
                    for(DnsLookupResult lookup : lookups.values()) status = AppCluster.max(status, lookup.getStatus().getResourceStatus());
                }
            }
        }

        return status;
    }
}
