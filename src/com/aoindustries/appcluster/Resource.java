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

import com.aoindustries.util.i18n.ThreadLocale;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Name;
import org.xbill.DNS.Record;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Type;

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
abstract public class Resource<R extends Resource<R,RN>,RN extends ResourceNode<R,RN>> {

    private static final Logger logger = Logger.getLogger(Resource.class.getName());

    private static final int DNS_MONITOR_THREAD_PRIORITY = Thread.NORM_PRIORITY - 1;

    /**
     * Checks the DNS settings once every 30 seconds.
     */
    private static final long DNS_CHECK_INTERVAL = 30000;

    /**
     * DNS queries time-out at 30 seconds.
     */
    private static final int DNS_CHECK_TIMEOUT = 30000;

    /**
     * Only one resolver will be created for each unique nameserver (case-insensitive on unique)
     */
    private static final ConcurrentMap<Name,SimpleResolver> resolvers = new ConcurrentHashMap<Name,SimpleResolver>();
    private static SimpleResolver getSimpleResolver(Name hostname) throws UnknownHostException {
        SimpleResolver resolver = resolvers.get(hostname);
        if(resolver==null) {
            resolver = new SimpleResolver(hostname.toString());
            resolver.setTimeout(DNS_CHECK_TIMEOUT / 1000, DNS_CHECK_TIMEOUT % 1000);
            SimpleResolver existing = resolvers.putIfAbsent(hostname, resolver);
            if(existing!=null) resolver = existing;
        }
        return resolver;
    }

    public static interface DnsStatusListener {
        void onDnsStatusChanged(DnsStatus oldDnsStatus, DnsStatus newDnsStatus);
    }

    private final List<DnsStatusListener> dnsStatusListeners = new ArrayList<DnsStatusListener>();

    private final AppCluster cluster;
    private final String id;
    private final boolean enabled;
    private final String display;
    private final Set<Name> masterRecords;
    private final boolean allowMultiMaster;

    public enum DnsStatus {
        STOPPED,
        DISABLED,
        UNKNOWN,
        INCONSISTENT,
        SLAVE,
        MASTER;

        @Override
        public String toString() {
            return ApplicationResources.accessor.getMessage("Resource.DnsStatus." + name());
        }
    }

    private final Object dnsMonitorLock = new Object();
    private Thread dnsMonitorThread; // All access uses dnsMonitorLock
    private DnsStatus dnsStatus = DnsStatus.STOPPED; // All access uses dnsMonitorLock
    private String dnsStatusMessage = null; // All access uses dnsMonitorLock

    Resource(AppCluster cluster, AppClusterConfiguration.ResourceConfiguration resourceConfiguration) {
        this.cluster = cluster;
        this.id = resourceConfiguration.getId();
        this.enabled = resourceConfiguration.isEnabled();
        this.display = resourceConfiguration.getDisplay();
        this.masterRecords = Collections.unmodifiableSet(new LinkedHashSet<Name>(resourceConfiguration.getMasterRecords()));
        this.allowMultiMaster = resourceConfiguration.getAllowMultiMaster();
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
     * Will be called when the DNS status has changed in any way.
     */
    public void addDnsStatusListener(DnsStatusListener listener) {
        synchronized(dnsStatusListeners) {
            boolean found = false;
            for(DnsStatusListener existing : dnsStatusListeners) {
                if(existing==listener) {
                    found = true;
                    break;
                }
            }
            if(!found) dnsStatusListeners.add(listener);
        }
    }

    /**
     * Removes listener of DNS status changes.
     */
    void removeDnsStatusListener(DnsStatusListener listener) {
        synchronized(dnsStatusListeners) {
            for(int i=0; i<dnsStatusListeners.size(); i++) {
                if(dnsStatusListeners.get(i)==listener) dnsStatusListeners.remove(i--);
            }
        }
    }

    // TODO: Start/stop the synchronization thread as needed
    private void setDnsStatus(DnsStatus newDnsStatus, String dnsStatusMessage) {
        assert Thread.holdsLock(dnsMonitorLock);
        DnsStatus oldDnsStatus = this.dnsStatus;
        this.dnsStatus = newDnsStatus;
        String oldDnsStatusMessage = this.dnsStatusMessage;
        this.dnsStatusMessage = dnsStatusMessage;
        // Notify listeners only when changed
        if(oldDnsStatus!=newDnsStatus) {
            if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("Resource.setDnsStatus.info", cluster, id, oldDnsStatus, newDnsStatus));
            synchronized(dnsStatusListeners) {
                for(DnsStatusListener dnsStatusListener : dnsStatusListeners) {
                    dnsStatusListener.onDnsStatusChanged(oldDnsStatus, newDnsStatus);
                }
            }
        }
        if(!dnsStatusMessage.equals(oldDnsStatusMessage)) {
            if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("Resource.setDnsStatus.newStatusMessage.info", cluster, id, dnsStatusMessage));
        }
    }

    /**
     * The unique ID of this resource.
     */
    public String getId() {
        return id;
    }

    /**
     * Determines if this resource is enabled.
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
     * Gets the set of master records that must all by the same.
     * The master node is determined by matching these records against
     * the resource node configuration's slave records.
     */
    public Set<Name> getMasterRecords() {
        return masterRecords;
    }

    /**
     * Gets if this resource allows multiple master servers.
     */
    public boolean getAllowMultiMaster() {
        return allowMultiMaster;
    }

    abstract public Map<Node,RN> getResourceNodes();

    private static final Name[] emptySearchPath = new Name[0];

    /**
     * If both the cluster and this node are enabled, starts the node monitor.
     */
    void start() {
        synchronized(dnsMonitorLock) {
            if(!cluster.isEnabled()) {
                setDnsStatus(DnsStatus.DISABLED, ApplicationResources.accessor.getMessage("Resource.start.clusterDisabled.statusMessage"));
            } else if(!isEnabled()) {
                setDnsStatus(DnsStatus.DISABLED, ApplicationResources.accessor.getMessage("Resource.start.resourceDisabled.statusMessage"));
            } else {
                if(dnsMonitorThread==null) {
                    setDnsStatus(DnsStatus.UNKNOWN, ApplicationResources.accessor.getMessage("Resource.start.newThread.statusMessage"));
                    final ExecutorService executorService = cluster.getExecutorService();
                    dnsMonitorThread = new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                final Thread currentThread = Thread.currentThread();
                                while(true) {
                                    try {
                                        synchronized(dnsMonitorLock) {
                                            if(currentThread!=dnsMonitorThread) break;
                                        }
                                        // Find all the unique hostnames and nameservers that will be queried
                                        Map<Node,RN> resourceNodes = getResourceNodes();
                                        int totalNameservers = 0;
                                        int totalHostnames = masterRecords.size();
                                        for(Map.Entry<Node,RN> entry : resourceNodes.entrySet()) {
                                            Node node = entry.getKey();
                                            if(node.isEnabled()) {
                                                totalNameservers += node.getNameservers().size();
                                                totalHostnames += entry.getValue().getSlaveRecords().size();
                                            }
                                        }
                                        Set<Name> allNameservers = new HashSet<Name>(totalNameservers*4/3+1);
                                        Set<Name> allHostnames = new HashSet<Name>(totalHostnames*4/3+1);
                                        allHostnames.addAll(masterRecords);
                                        for(Map.Entry<Node,RN> entry : resourceNodes.entrySet()) {
                                            Node node = entry.getKey();
                                            if(node.isEnabled()) {
                                                allNameservers.addAll(node.getNameservers());
                                                allHostnames.addAll(entry.getValue().getSlaveRecords());
                                            }
                                        }

                                        // Query all nameservers for all involved dns entries in parallel, getting all A records
                                        long queryStart = System.nanoTime();
                                        Map<Name,Map<Name,Future<Record[]>>> futures = new HashMap<Name,Map<Name,Future<Record[]>>>(allNameservers.size()*4/3+1);
                                        for(Name nameserver : allNameservers) {
                                            final SimpleResolver resolver = getSimpleResolver(nameserver);
                                            Map<Name,Future<Record[]>> hostnameFutures = new HashMap<Name,Future<Record[]>>(allHostnames.size()*4/3+1);
                                            for(final Name hostname : allHostnames) {
                                                hostnameFutures.put(
                                                    hostname,
                                                    executorService.submit(
                                                        new Callable<Record[]>() {
                                                            @Override
                                                            public Record[] call() {
                                                                Lookup lookup = new Lookup(hostname, Type.A);
                                                                lookup.setCache(null);
                                                                lookup.setResolver(resolver);
                                                                lookup.setSearchPath(emptySearchPath);
                                                                return lookup.run();
                                                            }
                                                        }
                                                    )
                                                );
                                            }
                                            futures.put(nameserver, hostnameFutures);
                                        }

                                        // Get all the results, ensuring consistency between multiple results.
                                        // If any single result is inconsistent, set inconsistent status.
                                        DnsStatus newDnsStatus = null;
                                        String newDnsStatusMessage = null;
                                        for(Name nameserver : allNameservers) {
                                            Map<Name,Future<Record[]>> hostnameFutures = new HashMap<Name,Future<Record[]>>(allHostnames.size()*4/3+1);
                                            // TODO
                                        }

                                        // TODO: Quorum for dns entries by nameserver counts?

                                        // TODO: Alert administrators on certain statuses
                                        
                                        // TODO: Log query time

                                        // TODO: Inconsistent if any A record is outside the expected slaveDomains

                                        // TODO: Each slaveDomain should resolve to only one A record?  Significance/Complications?
                                        // TODO: All multi-slave records must have the same IP address
                                        // TODO: All multi-master records must have the same IP address(es)
                                        // TODO: Make sure we got at least one response for every hostname
                                        // UNKNOWN if didn't get a response, this will override inconsistent
                                        
                                        // TODO: If consistent and at least one response, make sure there is
                                        // exactly one master if not allowMultiMaster.
                                        // to one of UNKNOWN, INCONSISTENT, SLAVE, or MASTER

                                        // TODO: Verify masterDomain TTL settings match expected values, issue as a warning

                                        // TODO: Get new status based on current DNS settings
                                        if(newDnsStatus==null) newDnsStatus = dnsStatus.UNKNOWN;
                                        if(newDnsStatusMessage==null) newDnsStatusMessage = "TODO";
                                        synchronized(dnsMonitorLock) {
                                            if(currentThread!=dnsMonitorThread) break;
                                            setDnsStatus(newDnsStatus, newDnsStatusMessage);
                                        }
                                    } catch(RejectedExecutionException exc) {
                                        // Normal during shutdown
                                        boolean needsLogged;
                                        synchronized(dnsMonitorLock) {
                                            needsLogged = currentThread==dnsMonitorThread;
                                        }
                                        if(needsLogged) logger.log(Level.SEVERE, null, exc);
                                    } catch(Exception exc) {
                                        logger.log(Level.SEVERE, null, exc);
                                    }
                                    try {
                                        Thread.sleep(DNS_CHECK_INTERVAL);
                                    } catch(InterruptedException exc) {
                                        logger.log(Level.WARNING, null, exc);
                                    }
                                }
                            }
                        },
                        "PropertiesConfiguration.fileMonitorThread"
                    );
                    dnsMonitorThread.setPriority(DNS_MONITOR_THREAD_PRIORITY);
                    dnsMonitorThread.start();
                }
            }
        }
    }

    /**
     * Stops this node monitor.
     */
    void stop(boolean isReloadingConfiguration) {
        synchronized(dnsMonitorLock) {
            dnsMonitorThread = null;
            if(isReloadingConfiguration) setDnsStatus(DnsStatus.STOPPED, ApplicationResources.accessor.getMessage("Resource.stop.reloadingConfiguration.statusMessage"));
            else setDnsStatus(DnsStatus.STOPPED, ApplicationResources.accessor.getMessage("Resource.stop.statusMessage"));
        }
    }

    /**
     * Gets the current status of this node.
     */
    public DnsStatus getDnsStatus() {
        synchronized(dnsMonitorLock) {
            return dnsStatus;
        }
    }

    /**
     * Gets the current DNS status message in the ThreadLocale of the user.
     *
     * @see ThreadLocale
     */
    public String getDnsStatusMessage() {
        synchronized(dnsMonitorLock) {
            return dnsStatusMessage;
        }
    }
}
