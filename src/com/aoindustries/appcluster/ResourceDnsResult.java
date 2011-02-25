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

/**
 * Contains the results of one DNS monitoring pass.
 *
 * @author  AO Industries, Inc.
 */
public class ResourceDnsResult {

    // private static final Logger logger = Logger.getLogger(ResourceDnsResult.class.getName());

    private static final Comparator<Object> collator = Collator.getInstance();

    private static SortedSet<String> getUnmodifiableSortedSet(Collection<String> collection) {
        if(collection==null || collection.isEmpty()) return com.aoindustries.util.Collections.emptySortedSet();
        if(collection.size()==1) return com.aoindustries.util.Collections.singletonSortedSet(collection.iterator().next());
        SortedSet<String> sortedSet = new TreeSet<String>(collator);
        sortedSet.addAll(collection);
        return Collections.unmodifiableSortedSet(sortedSet);
    }

    private final Resource<?,?> resource;
    private final long startTime;
    private final long endTime;
    private final MasterDnsStatus masterStatus;
    private final SortedSet<String> masterStatusMessages;
    private final Map<Node,NodeDnsStatus> nodeStatuses;
    private final Map<Node,SortedSet<String>> nodeStatusMessages;
    private final SortedSet<String> warnings;
    private final SortedSet<String> errors;

    ResourceDnsResult(
        Resource<?,?> resource,
        long startTime,
        long endTime,
        MasterDnsStatus masterStatus,
        Collection<String> masterStatusMessages,
        Map<Node,NodeDnsStatus> nodeStatuses,
        Map<Node,? extends Collection<String>> nodeStatusMessages,
        Collection<String> warnings,
        Collection<String> errors
    ) {
        this.resource = resource;
        this.startTime = startTime;
        this.endTime = endTime;
        this.masterStatus = masterStatus;
        this.masterStatusMessages = getUnmodifiableSortedSet(masterStatusMessages);
        Set<Node> nodes = resource.getResourceNodes().keySet();
        Map<Node,NodeDnsStatus> newNodeStatuses = new LinkedHashMap<Node,NodeDnsStatus>(nodes.size()*4/3+1);
        Map<Node,SortedSet<String>> newNodeStatusMessages = new LinkedHashMap<Node,SortedSet<String>>(nodes.size()*4/3+1);
        for(Node node : nodes) {
            NodeDnsStatus nodeStatus = nodeStatuses.get(node);
            if(nodeStatus==null) throw new IllegalArgumentException("Missing node " + node);
            newNodeStatuses.put(node, nodeStatus);
            newNodeStatusMessages.put(node, getUnmodifiableSortedSet(nodeStatusMessages==null ? null : nodeStatusMessages.get(node)));
        }
        this.nodeStatuses = Collections.unmodifiableMap(newNodeStatuses);
        this.nodeStatusMessages = Collections.unmodifiableMap(newNodeStatusMessages);
        this.warnings = getUnmodifiableSortedSet(warnings);
        this.errors = getUnmodifiableSortedSet(errors);
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
     * Gets the current status of the master records.
     */
    public MasterDnsStatus getMasterStatus() {
        return masterStatus;
    }

    /**
     * Gets the current master status messages.
     * If no message, returns an empty set.
     */
    public SortedSet<String> getMasterStatusMessages() {
        return masterStatusMessages;
    }

    /**
     * Gets the current status of each node.
     * This has an entry for every node in this resource.
     */
    public Map<Node,NodeDnsStatus> getNodeStatuses() {
        return nodeStatuses;
    }

    /**
     * Gets the current DNS status message on a per-node basis.
     * This has an entry for every node in this resource, even if it is an empty set of messages.
     */
    public Map<Node,SortedSet<String>> getNodeStatusMessages() {
        return nodeStatusMessages;
    }

    /**
     * Gets the most recent warnings for this resource.
     */
    public SortedSet<String> getWarnings() {
        return warnings;
    }

    /**
     * Gets the most recent errors for this resource.
     */
    public SortedSet<String> getErrors() {
        return errors;
    }
}
