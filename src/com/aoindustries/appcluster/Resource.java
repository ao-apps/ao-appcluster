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

import com.aoindustries.util.StringUtility;
import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xbill.DNS.ARecord;
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
 * TODO: Alert administrators on certain statuses
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
    private final boolean allowMultiMaster;
    private final Set<Name> masterRecords;
    private final int masterRecordsTtl;

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
    private List<String> warnings = null; // All access uses dnsMonitorLock
    private List<String> errors = null; // All access uses dnsMonitorLock

    Resource(AppCluster cluster, AppClusterConfiguration.ResourceConfiguration resourceConfiguration) {
        this.cluster = cluster;
        this.id = resourceConfiguration.getId();
        this.enabled = resourceConfiguration.isEnabled();
        this.display = resourceConfiguration.getDisplay();
        this.allowMultiMaster = resourceConfiguration.getAllowMultiMaster();
        this.masterRecords = Collections.unmodifiableSet(new LinkedHashSet<Name>(resourceConfiguration.getMasterRecords()));
        this.masterRecordsTtl = resourceConfiguration.getMasterRecordsTtl();
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
    private void setDnsStatus(DnsStatus newDnsStatus, String dnsStatusMessage, Set<String> warnings, Set<String> errors) {
        assert Thread.holdsLock(dnsMonitorLock);
        DnsStatus oldDnsStatus = this.dnsStatus;
        this.dnsStatus = newDnsStatus;
        String oldDnsStatusMessage = this.dnsStatusMessage;
        this.dnsStatusMessage = dnsStatusMessage;
        List<String> oldWarnings = this.warnings;
        if(warnings==null) this.warnings = Collections.emptyList();
        else {
            List<String> sortedWarnings = new ArrayList<String>(warnings);
            Collections.sort(sortedWarnings);
            this.warnings = Collections.unmodifiableList(sortedWarnings);
        }
        List<String> oldErrors = this.errors;
        if(errors==null) this.errors = Collections.emptyList();
        else {
            List<String> sortedErrors = new ArrayList<String>(errors);
            Collections.sort(sortedErrors);
            this.errors = Collections.unmodifiableList(sortedErrors);
        }
        // Notify listeners only when changed
        if(oldDnsStatus!=newDnsStatus) {
            if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("Resource.setDnsStatus.info", cluster, id, oldDnsStatus, newDnsStatus));
            synchronized(dnsStatusListeners) {
                for(DnsStatusListener dnsStatusListener : dnsStatusListeners) {
                    dnsStatusListener.onDnsStatusChanged(oldDnsStatus, newDnsStatus);
                }
            }
        }
        if(dnsStatusMessage!=null && !dnsStatusMessage.equals(oldDnsStatusMessage)) {
            if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("Resource.setDnsStatus.newStatusMessage.info", cluster, id, dnsStatusMessage));
        }
        if(!this.warnings.equals(oldWarnings)) {
            for(String warning : this.warnings) {
                if(logger.isLoggable(Level.WARNING)) logger.warning(ApplicationResources.accessor.getMessage("Resource.setDnsStatus.warning", cluster, id, warning));
            }
        }
        if(!this.errors.equals(oldErrors)) {
            for(String error : this.errors) {
                if(logger.isLoggable(Level.SEVERE)) logger.severe(ApplicationResources.accessor.getMessage("Resource.setDnsStatus.error", cluster, id, error));
            }
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
     * Gets if this resource allows multiple master servers.
     */
    public boolean getAllowMultiMaster() {
        return allowMultiMaster;
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
     * Gets the expected TTL value for the master record.
     */
    public int getMasterRecordTtl() {
        return masterRecordsTtl;
    }

    abstract public Map<Node,RN> getResourceNodes();

    private static final Name[] emptySearchPath = new Name[0];

    /**
     * If both the cluster and this node are enabled, starts the node monitor.
     */
    void start() {
        synchronized(dnsMonitorLock) {
            if(!cluster.isEnabled()) {
                setDnsStatus(DnsStatus.DISABLED, ApplicationResources.accessor.getMessage("Resource.start.clusterDisabled.statusMessage"), null, null);
            } else if(!isEnabled()) {
                setDnsStatus(DnsStatus.DISABLED, ApplicationResources.accessor.getMessage("Resource.start.resourceDisabled.statusMessage"), null, null);
            } else {
                if(dnsMonitorThread==null) {
                    setDnsStatus(DnsStatus.UNKNOWN, ApplicationResources.accessor.getMessage("Resource.start.newThread.statusMessage"), null, null);
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
                                        // Add any errors or warnings to the lists and return null if unable to get A records.
                                        long queryStart = System.nanoTime();
                                        final Set<String> newWarnings = Collections.synchronizedSet(new HashSet<String>());
                                        final Set<String> newErrors = Collections.synchronizedSet(new HashSet<String>());
                                        Map<Name,Map<Name,Future<String[]>>> futures = new HashMap<Name,Map<Name,Future<String[]>>>(allNameservers.size()*4/3+1);
                                        for(final Name nameserver : allNameservers) {
                                            final SimpleResolver resolver = getSimpleResolver(nameserver);
                                            Map<Name,Future<String[]>> hostnameFutures = new HashMap<Name,Future<String[]>>(allHostnames.size()*4/3+1);
                                            for(final Name hostname : allHostnames) {
                                                hostnameFutures.put(
                                                    hostname,
                                                    executorService.submit(
                                                        new Callable<String[]>() {
                                                            @Override
                                                            public String[] call() {
                                                                Lookup lookup = new Lookup(hostname, Type.A);
                                                                lookup.setCache(null);
                                                                lookup.setResolver(resolver);
                                                                lookup.setSearchPath(emptySearchPath);
                                                                Record[] records = lookup.run();
                                                                int result = lookup.getResult();
                                                                switch(result) {
                                                                    case Lookup.SUCCESSFUL :
                                                                        if(records==null || records.length==0) {
                                                                            newErrors.add(ApplicationResources.accessor.getMessage("Resource.lookup.HOST_NOT_FOUND", nameserver, hostname));
                                                                            return null;
                                                                        }
                                                                        String[] addresses = new String[records.length];
                                                                        for(int c=0;c<records.length;c++) {
                                                                            ARecord aRecord = (ARecord)records[c];
                                                                            // Verify masterDomain TTL settings match expected values, issue as a warning
                                                                            if(masterRecords.contains(hostname)) {
                                                                                long ttl = aRecord.getTTL();
                                                                                if(ttl!=masterRecordsTtl) newWarnings.add(ApplicationResources.accessor.getMessage("Resource.lookup.unexpectedTtl", nameserver, hostname, masterRecordsTtl, ttl));
                                                                            }
                                                                            addresses[c] = aRecord.getAddress().getHostAddress();
                                                                        }
                                                                        // Sort all addresses returned, so we may easily compare for equality
                                                                        if(addresses.length>1) Arrays.sort(addresses);
                                                                        return addresses;
                                                                    case Lookup.UNRECOVERABLE :
                                                                        newErrors.add(ApplicationResources.accessor.getMessage("Resource.lookup.UNRECOVERABLE", nameserver, hostname));
                                                                        return null;
                                                                    case Lookup.TRY_AGAIN :
                                                                        newWarnings.add(ApplicationResources.accessor.getMessage("Resource.lookup.TRY_AGAIN", nameserver, hostname));
                                                                        return null;
                                                                    case Lookup.HOST_NOT_FOUND :
                                                                        newErrors.add(ApplicationResources.accessor.getMessage("Resource.lookup.HOST_NOT_FOUND", nameserver, hostname));
                                                                        return null;
                                                                    case Lookup.TYPE_NOT_FOUND :
                                                                        newErrors.add(ApplicationResources.accessor.getMessage("Resource.lookup.TYPE_NOT_FOUND", nameserver, hostname));
                                                                        return null;
                                                                    default :
                                                                        newErrors.add(ApplicationResources.accessor.getMessage("Resource.lookup.unexpectedResultCode", nameserver, hostname, result));
                                                                        return null;
                                                                }
                                                            }
                                                        }
                                                    )
                                                );
                                            }
                                            futures.put(nameserver, hostnameFutures);
                                        }

                                        // Get all the results, ensuring consistency between multiple results.
                                        // If any single result is inconsistent (not exactly the same set of A records), set inconsistent status and message.
                                        Map<Name,String[]> aRecords = new HashMap<Name,String[]>(allHostnames.size()*4/3+1);
                                        DnsStatus newDnsStatus = null;
                                        String newDnsStatusMessage = null;
                                        for(Name nameserver : allNameservers) {
                                            Map<Name,Future<String[]>> hostnameFutures = futures.get(nameserver);
                                            for(Name hostname : allHostnames) {
                                                try {
                                                    String[] records = hostnameFutures.get(hostname).get();
                                                    if(records!=null) {
                                                        String[] existing = aRecords.get(hostname);
                                                        if(existing==null) aRecords.put(hostname, records);
                                                        else if(!Arrays.equals(existing, records)) {
                                                            if(newDnsStatus==null) {
                                                                newDnsStatus = DnsStatus.INCONSISTENT;
                                                                newDnsStatusMessage = ApplicationResources.accessor.getMessage(
                                                                    "Resource.aRecords.inconsistent",
                                                                    nameserver,
                                                                    hostname,
                                                                    StringUtility.buildList(existing),
                                                                    StringUtility.buildList(records)
                                                                );
                                                            }
                                                        }
                                                    }
                                                } catch(ExecutionException exc) {
                                                    logger.log(Level.SEVERE, null, exc);
                                                    newErrors.add(exc.toString());
                                                } catch(InterruptedException exc) {
                                                    // Normal during shutdown
                                                    boolean needsLogged;
                                                    synchronized(dnsMonitorLock) {
                                                        needsLogged = currentThread==dnsMonitorThread;
                                                    }
                                                    if(needsLogged) {
                                                        logger.log(Level.WARNING, null, exc);
                                                        newWarnings.add(exc.toString());
                                                    }
                                                }
                                            }
                                        }

                                        // Log query time
                                        long timeNanos = System.nanoTime() - queryStart;
                                        if(logger.isLoggable(Level.FINE)) logger.fine(ApplicationResources.accessor.getMessage("Resource.allQueries.timeMillis", cluster, id, BigDecimal.valueOf(timeNanos, 6)));

                                        // Make sure we got at least one response for every master and check multi-master support
                                        if(newDnsStatus==null) {
                                            Name firstRecord = null;
                                            String[] firstAddresses = null;
                                            for(Name masterRecord : masterRecords) {
                                                String[] addresses = aRecords.get(masterRecord);
                                                if(addresses==null || addresses.length==0) {
                                                    newDnsStatus = DnsStatus.INCONSISTENT;
                                                    newDnsStatusMessage = ApplicationResources.accessor.getMessage("Resource.masterRecord.missing", masterRecord);
                                                    break;
                                                }
                                                // Check for multi-master violation
                                                if(addresses.length>1 && !allowMultiMaster) {
                                                    newDnsStatus = DnsStatus.INCONSISTENT;
                                                    newDnsStatusMessage = ApplicationResources.accessor.getMessage("Resource.masterRecord.multiMasterNotAllowed", masterRecord, StringUtility.buildList(addresses));
                                                    break;
                                                }
                                                // All multi-record masters must have the same IP address(es) within a single node (like for domain aliases)
                                                if(firstRecord==null) {
                                                    firstRecord = masterRecord;
                                                    firstAddresses = addresses;
                                                } else if(!Arrays.equals(firstAddresses, addresses)) {
                                                    newDnsStatus = DnsStatus.INCONSISTENT;
                                                    newDnsStatusMessage = ApplicationResources.accessor.getMessage(
                                                        "Resource.multiRecordMaster.mismatch",
                                                        firstRecord,
                                                        StringUtility.buildList(firstAddresses),
                                                        masterRecord,
                                                        StringUtility.buildList(addresses)
                                                    );
                                                    break;
                                                }
                                            }
                                        }

                                        // Make sure we got one and only one response for every slave
                                        Map<String,Name> slaveAddresses = new HashMap<String,Name>(); // Will be incomplete when newDnsStatus is set
                                        if(newDnsStatus==null) {

                                            RN_LOOP:
                                            for(Map.Entry<Node,RN> entry : resourceNodes.entrySet()) {
                                                if(entry.getKey().isEnabled()) {
                                                    Name firstRecord = null;
                                                    String[] firstAddresses = null;
                                                    for(Name slaveRecord : entry.getValue().getSlaveRecords()) {
                                                        String[] addresses = aRecords.get(slaveRecord);
                                                        if(addresses==null || addresses.length==0) {
                                                            newDnsStatus = DnsStatus.INCONSISTENT;
                                                            newDnsStatusMessage = ApplicationResources.accessor.getMessage("Resource.slaveRecord.missing", slaveRecord);
                                                            break RN_LOOP;
                                                        }
                                                        // Must be only one A record
                                                        if(addresses.length>1) {
                                                            newDnsStatus = DnsStatus.INCONSISTENT;
                                                            newDnsStatusMessage = ApplicationResources.accessor.getMessage("Resource.slaveRecord.onlyOneAllowed", slaveRecord, StringUtility.buildList(addresses));
                                                            break RN_LOOP;
                                                        }
                                                        // All multi-record slaves must have the same IP address(es) within a single node (like for domain aliases)
                                                        if(firstRecord==null) {
                                                            firstRecord = slaveRecord;
                                                            firstAddresses = addresses;
                                                            // Each slave must have a different A record
                                                            Name duplicateSlave = slaveAddresses.put(addresses[0], slaveRecord);
                                                            if(duplicateSlave!=null) {
                                                                newDnsStatus = DnsStatus.INCONSISTENT;
                                                                newDnsStatusMessage = ApplicationResources.accessor.getMessage("Resource.slaveRecord.duplicateA", duplicateSlave, slaveRecord, addresses[0]);
                                                                break RN_LOOP;
                                                            }
                                                        } else if(!Arrays.equals(firstAddresses, addresses)) {
                                                            newDnsStatus = DnsStatus.INCONSISTENT;
                                                            newDnsStatusMessage = ApplicationResources.accessor.getMessage(
                                                                "Resource.multiRecordSlave.mismatch",
                                                                firstRecord,
                                                                StringUtility.buildList(firstAddresses),
                                                                slaveRecord,
                                                                StringUtility.buildList(addresses)
                                                            );
                                                            break RN_LOOP;
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        if(newDnsStatus==null) {
                                            // Inconsistent if any master A record is outside the expected slaveDomains
                                        MASTER_LOOP :
                                            for(Name masterRecord : masterRecords) {
                                                for(String address : aRecords.get(masterRecord)) {
                                                    if(!slaveAddresses.containsKey(address)) {
                                                        newDnsStatus = DnsStatus.INCONSISTENT;
                                                        newDnsStatusMessage = ApplicationResources.accessor.getMessage(
                                                            "Resource.masterARecordDoesntMatchSlave",
                                                            masterRecord,
                                                            address
                                                        );
                                                        break MASTER_LOOP;
                                                    }
                                                }
                                            }
                                        }

                                        // TODO: Now one of SLAVE or MASTER depending on hostname match
                                        //       TODO: This host must be one of the nodes

                                        if(newDnsStatus==null) newDnsStatus = dnsStatus.UNKNOWN;
                                        synchronized(dnsMonitorLock) {
                                            if(currentThread!=dnsMonitorThread) break;
                                            setDnsStatus(newDnsStatus, newDnsStatusMessage, newWarnings, newErrors);
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
            if(isReloadingConfiguration) setDnsStatus(DnsStatus.STOPPED, ApplicationResources.accessor.getMessage("Resource.stop.reloadingConfiguration.statusMessage"), null, null);
            else setDnsStatus(DnsStatus.STOPPED, ApplicationResources.accessor.getMessage("Resource.stop.statusMessage"), null, null);
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
     * Gets the current DNS status message or <code>null</code> if none.
     *
     * @see ThreadLocale
     */
    public String getDnsStatusMessage() {
        synchronized(dnsMonitorLock) {
            return dnsStatusMessage;
        }
    }

    /**
     * Gets the most recent warnings for this resource.
     */
    public List<String> getWarnings() {
        synchronized(dnsMonitorLock) {
            if(warnings==null) return Collections.emptyList();
            else return warnings;
        }
    }

    /**
     * Gets the most recent errors for this resource.
     */
    public List<String> getErrors() {
        synchronized(dnsMonitorLock) {
            if(errors==null) return Collections.emptyList();
            else return errors;
        }
    }
}
