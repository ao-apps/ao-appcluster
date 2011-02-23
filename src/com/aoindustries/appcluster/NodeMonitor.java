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
 * Monitors the status of a node given its DNS entries.
 *
 * @author  AO Industries, Inc.
 */
public class NodeMonitor {

    private static final Logger logger = Logger.getLogger(NodeMonitor.class.getName());

    private final AppCluster cluster;
    private final String id;
    private final boolean enabled;
    private final String display;
    private final String hostname;
    private final Set<String> nameservers;

    public enum Status {
        STOPPED,
        DISABLED,
        UNKNOWN;

        @Override
        public String toString() {
            return ApplicationResources.accessor.getMessage("NodeMonitor.Status." + name());
        }
    }

    private final Object nodeMonitorLock = new Object();
    private Thread nodeMonitorThread; // All access uses nodeMonitorLock
    private Status status = Status.STOPPED; // All access uses nodeMonitorLock

    public NodeMonitor(AppCluster cluster, AppClusterConfiguration.NodeConfiguration nodeConfiguration) throws IllegalArgumentException {
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
    public void start() {
        synchronized(nodeMonitorLock) {
            if(cluster.isEnabled() && isEnabled()) {
                if(nodeMonitorThread==null) {
                    status = Status.UNKNOWN;
                    (
                        nodeMonitorThread = new Thread(
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
                                            synchronized(nodeMonitorLock) {
                                                if(currentThread!=nodeMonitorThread) break;
                                            }
                                            // TODO: Get new status
                                            Status newStatus = status.UNKNOWN;
                                            boolean notifyListeners;
                                            synchronized(nodeMonitorLock) {
                                                if(currentThread!=nodeMonitorThread) break;
                                                if(newStatus!=status) {
                                                    notifyListeners = true;
                                                    status = newStatus;
                                                }
                                            }
                                            if(notifyListeners) {
                                                synchronized(listeners) {
                                                    for(NodeStatusChangeListener listener : listeners) listener.onNodeStatusChanged(newStatus);
                                                }
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
    public void stop() {
        synchronized(nodeMonitorLock) {
            nodeMonitorThread = null;
            status = Status.STOPPED;
        }
    }

    /**
     * Gets the current status of this node.
     */
    public Status getStatus() {
        synchronized(nodeMonitorLock) {
            return status;
        }
    }
}
