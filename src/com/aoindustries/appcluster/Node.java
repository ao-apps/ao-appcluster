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
import java.util.Set;
import org.xbill.DNS.Name;

/**
 * One node within the cluster.
 *
 * @author  AO Industries, Inc.
 */
public class Node {

    // private static final Logger logger = Logger.getLogger(Node.class.getName());

    private final AppCluster cluster;
    private final String id;
    private final boolean enabled;
    private final String display;
    private final Name hostname;
    private final Set<Name> nameservers;

    Node(AppCluster cluster, AppClusterConfiguration.NodeConfiguration nodeConfiguration) {
        this.cluster = cluster;
        this.id = nodeConfiguration.getId();
        this.enabled = cluster.isEnabled() && nodeConfiguration.isEnabled();
        this.display = nodeConfiguration.getDisplay();
        this.hostname = nodeConfiguration.getHostname();
        this.nameservers = Collections.unmodifiableSet(new LinkedHashSet<Name>(nodeConfiguration.getNameservers()));
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
     * Gets the set of nameservers that are local to the machine running this node.
     */
    public Set<Name> getNameservers() {
        return nameservers;
    }
}
