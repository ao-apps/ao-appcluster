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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Monitors the status of a resource by monitoring its role based on DNS entries
 * and synchronizing the resource on an as-needed and/or scheduled basis.
 *
 * There are two threads involved in the resource monitoring.  The first
 * monitors DNS entries to determine if this node is the master or a slave (or
 * some inconsistent state in-between).
 *
 * The second thread only runs when we are a master with no DNS inconsistencies.
 * It pushes the resources to slaves on an as-needed and/or scheduled basis.
 *
 * @author  AO Industries, Inc.
 */
public class ResourceMonitor {

    private static final Logger logger = Logger.getLogger(ResourceMonitor.class.getName());

    /**
     * Checks the DNS settings once every 30 seconds.
     */
    private static final long DNS_CHECK_INTERVAL = 30000;

    /**
     * DNS queries time-out at 30 seconds.
     */
    private static final long DNS_CHECK_TIMEOUT = 30000;

    private final AppCluster cluster;
    private final String id;
    private final boolean enabled;
    private final String display;
    private final String hostname;
    private final Set<String> nameservers;

    public enum Status {
        STOPPED,
        DISABLED,
        UNKNOWN,
        INCONSISTENT_DNS,
        SLAVE,
        MASTER;

        @Override
        public String toString() {
            return ApplicationResources.accessor.getMessage("ResourceMonitor.Status." + name());
        }
    }

    private final Object resourceMonitorLock = new Object();
    private Thread resourceMonitorThread; // All access uses resourceMonitorLock
    private Status status = Status.STOPPED; // All access uses resourceMonitorLock

    ResourceMonitor(AppCluster cluster, AppClusterConfiguration.ResourceConfiguration resourceConfiguration) {
        this.cluster = cluster;
        this.id = nodeConfiguration.getId();
        this.enabled = nodeConfiguration.isEnabled();
        this.display = nodeConfiguration.getDisplay();
        this.hostname = nodeConfiguration.getHostname();
        this.nameservers = Collections.unmodifiableSet(new LinkedHashSet<String>(nodeConfiguration.getNameservers()));
    }

    /**
     * Gets the unique identifier for this node.
     */
    public String getId() {
        return id;
    }

    /**
     * Determines if this node is enabled.
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
    public String getHostname() {
        return hostname;
    }

    /**
     * Gets the set of nameservers that are local to the machine running this node.
     */
    public Set<String> getNameservers() {
        return nameservers;
    }

    /**
     * If both the cluster and this node are enabled, starts the node monitor.
     */
    void start() {
        synchronized(resourceMonitorLock) {
            if(cluster.isEnabled() && isEnabled()) {
                if(resourceMonitorThread==null) {
                    status = Status.UNKNOWN;
                    (
                        resourceMonitorThread = new Thread(
                            new Runnable() {
                                @Override
                                public void run() {
                                    final Thread currentThread = Thread.currentThread();
                                    while(true) {
                                        try {
                                            try {
                                                Thread.sleep(DNS_CHECK_INTERVAL);
                                            } catch(InterruptedException exc) {
                                                logger.log(Level.WARNING, null, exc);
                                            }
                                            synchronized(resourceMonitorLock) {
                                                if(currentThread!=resourceMonitorThread) break;
                                            }
                                            // TODO: Get new status
                                            Status newStatus = status.UNKNOWN;
                                            boolean notifyListeners = false;
                                            synchronized(resourceMonitorLock) {
                                                if(currentThread!=resourceMonitorThread) break;
                                                if(newStatus!=status) {
                                                    notifyListeners = true;
                                                    status = newStatus;
                                                }
                                            }
                                            if(notifyListeners) {
                                                /* TODO
                                                synchronized(listeners) {
                                                    for(NodeStatusChangeListener listener : listeners) listener.onNodeStatusChanged(newStatus);
                                                }
                                                 */
                                            }
                                        } catch(Exception exc) {
                                            logger.log(Level.SEVERE, null, exc);
                                        }
                                    }
                                }
                            },
                            "PropertiesConfiguration.fileMonitorThread"
                        )
                    ).start();
                }
            } else {
                status = Status.DISABLED;
            }
        }
    }

    /**
     * Stops this node monitor.
     */
    void stop() {
        synchronized(resourceMonitorLock) {
            resourceMonitorThread = null;
            status = Status.STOPPED;
        }
    }

    /**
     * Gets the current status of this node.
     */
    public Status getStatus() {
        synchronized(resourceMonitorLock) {
            return status;
        }
    }
}
