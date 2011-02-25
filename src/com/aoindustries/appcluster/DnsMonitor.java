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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
 * Monitors DNS entries to determine which nodes are masters and which are slaves
 * while being careful to detect any inconsistent states.
 *
 * @author  AO Industries, Inc.
 */
public class DnsMonitor<R extends Resource<R,RN>,RN extends ResourceNode<R,RN>> {

    private static final Logger logger = Logger.getLogger(DnsMonitor.class.getName());

    private static final int THREAD_PRIORITY = Thread.NORM_PRIORITY - 1;

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

    private final Resource<R,RN> resource;

    private final Object threadLock = new Object();
    private Thread thread; // All access uses threadLock
    private DnsResult lastResult; // All access uses threadLock

    private final List<DnsListener> listeners = new ArrayList<DnsListener>();

    DnsMonitor(Resource<R,RN> resource) {
        this.resource = resource;
        long currentTime = System.currentTimeMillis();
        this.lastResult = new DnsResult(currentTime, currentTime, DnsStatus.STOPPED, ApplicationResources.accessor.getMessage("DnsMonitor.stop.statusMessage"), null, null);
    }

    /**
     * Gets the resource this monitor is for.
     */
    public Resource<R,RN> getResource() {
        return resource;
    }

    /**
     * Will be called when the DNS result has changed in any way.
     */
    public void addDnsListener(DnsListener listener) {
        synchronized(listeners) {
            boolean found = false;
            for(DnsListener existing : listeners) {
                if(existing==listener) {
                    found = true;
                    break;
                }
            }
            if(!found) listeners.add(listener);
        }
    }

    /**
     * Removes listener of DNS result changes.
     */
    public void removeDnsListener(DnsListener listener) {
        synchronized(listeners) {
            for(int i=0; i<listeners.size(); i++) {
                if(listeners.get(i)==listener) {
                    listeners.remove(i);
                    break;
                }
            }
        }
    }

    private static final String eol = System.getProperty("line.separator");

    /**
     * Appends to a StringBuilder, creating it if necessary, adding a newline to separate the new message.
     */
    private static StringBuilder appendWithNewline(StringBuilder message, String line) {
        if(message==null) message = new StringBuilder(line);
        else {
            if(message.length()>0) message.append(eol);
            message.append(line);
        }
        return message;
    }

    // TODO: Start/stop the synchronization thread as needed
    // TODO: Make sure this is only called while started (check thread==currentThread inside lock first)
    private void setDnsResult(DnsResult newResult) {
        assert Thread.holdsLock(threadLock);
        DnsResult oldResult = this.lastResult;
        this.lastResult = newResult;

        // Log any changes, except continual changes to time
        if(logger.isLoggable(Level.FINE)) {
            logger.fine(ApplicationResources.accessor.getMessage("DnsMonitor.allQueries.timeMillis", resource.getCluster(), resource, newResult.getEndTime() - newResult.getStartTime()));
        }
        StringBuilder message = null; // Remains null when no messages are generated
        if(logger.isLoggable(Level.INFO)) {
            if(message!=null) message.setLength(0);
            if(newResult.getStatus()!=oldResult.getStatus()) {
                message = appendWithNewline(message, ApplicationResources.accessor.getMessage("DnsMonitor.setDnsResult.info", resource.getCluster(), resource, oldResult.getStatus(), newResult.getStatus()));
            }
            if(newResult.getStatusMessage()!=null && !newResult.getStatusMessage().equals(oldResult.getStatusMessage())) {
                message = appendWithNewline(message, ApplicationResources.accessor.getMessage("DnsMonitor.setDnsResult.newStatusMessage.info", resource.getCluster(), resource, newResult.getStatusMessage()));
            }
            if(message!=null && message.length()>0) logger.info(message.toString());
        }
        if(logger.isLoggable(Level.WARNING)) {
            if(message!=null) message.setLength(0);
            if(!newResult.getWarnings().equals(oldResult.getWarnings())) {
                for(String warning : newResult.getWarnings()) {
                    message = appendWithNewline(message, ApplicationResources.accessor.getMessage("DnsMonitor.setDnsResult.warning", resource.getCluster(), resource, warning));
                }
            }
            if(message!=null && message.length()>0) message.setLength(0);
        }
        if(logger.isLoggable(Level.SEVERE)) {
            if(message!=null) message.setLength(0);
            if(!newResult.getErrors().equals(oldResult.getErrors())) {
                for(String error : newResult.getErrors()) {
                    message = appendWithNewline(message, ApplicationResources.accessor.getMessage("DnsMonitor.setDnsResult.error", resource.getCluster(), resource, error));
                }
            }
            if(message!=null && message.length()>0) logger.severe(message.toString());
        }

        // Notify listeners
        synchronized(listeners) {
            for(DnsListener listener : listeners) listener.onDnsResult(oldResult, newResult);
        }
    }

    /**
     * Gets the last result.
     */
    public DnsResult getLastResult() {
        synchronized(threadLock) {
            return lastResult;
        }
    }

    private static final Name[] emptySearchPath = new Name[0];

    /**
     * If both the cluster and this node are enabled, starts the node monitor.
     */
    void start() {
        synchronized(threadLock) {
            if(!resource.getCluster().isEnabled()) {
                long currentTime = System.currentTimeMillis();
                setDnsResult(new DnsResult(currentTime, currentTime, DnsStatus.DISABLED, ApplicationResources.accessor.getMessage("DnsMonitor.start.clusterDisabled.statusMessage"), null, null));
            } else if(!resource.isEnabled()) {
                long currentTime = System.currentTimeMillis();
                setDnsResult(new DnsResult(currentTime, currentTime, DnsStatus.DISABLED, ApplicationResources.accessor.getMessage("DnsMonitor.start.resourceDisabled.statusMessage"), null, null));
            } else {
                if(thread==null) {
                    long currentTime = System.currentTimeMillis();
                    setDnsResult(new DnsResult(currentTime, currentTime, DnsStatus.UNKNOWN, ApplicationResources.accessor.getMessage("DnsMonitor.start.newThread.statusMessage"), null, null));
                    final ExecutorService executorService = resource.getCluster().getExecutorService();
                    thread = new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                final Thread currentThread = Thread.currentThread();
                                final Set<Name> masterRecords = resource.getMasterRecords();
                                final int masterRecordsTtl = resource.getMasterRecordTtl();
                                final boolean allowMultiMaster = resource.getAllowMultiMaster();

                                @SuppressWarnings("unchecked")
                                final ResourceNode<R,RN>[] resourceNodes = resource.getResourceNodes().values().toArray(new ResourceNode[resource.getResourceNodes().size()]);

                                // Find all the unique hostnames and nameservers that will be queried
                                final Name[] allNameservers;
                                final Name[] allHostnames;
                                {
                                    final Set<Name> allNameserversSet = new HashSet<Name>();
                                    final Set<Name> allHostnamesSet = new HashSet<Name>();
                                    allHostnamesSet.addAll(masterRecords);
                                    for(ResourceNode<R,RN> resourceNode : resourceNodes) {
                                        Node node = resourceNode.getNode();
                                        if(node.isEnabled()) {
                                            allNameserversSet.addAll(node.getNameservers());
                                            allHostnamesSet.addAll(resourceNode.getNodeRecords());
                                        }
                                    }
                                    allNameservers = allNameserversSet.toArray(new Name[allNameserversSet.size()]);
                                    allHostnames = allHostnamesSet.toArray(new Name[allHostnamesSet.size()]);
                                }

                                // These objects are reused within the loop
                                final List<String> newWarnings = Collections.synchronizedList(new ArrayList<String>());
                                final List<String> newErrors = Collections.synchronizedList(new ArrayList<String>());
                                final Map<Name,Map<Name,Future<String[]>>> futures = new HashMap<Name,Map<Name,Future<String[]>>>(allNameservers.length*4/3+1);
                                for(Name nameserver : allNameservers) {
                                    Map<Name,Future<String[]>> hostnameFutures = new HashMap<Name,Future<String[]>>(allHostnames.length*4/3+1);
                                    for(Name hostname : allHostnames) hostnameFutures.put(hostname, null);
                                    futures.put(nameserver, hostnameFutures);
                                }
                                final Map<Name,String[]> aRecords = new HashMap<Name,String[]>(allHostnames.length*4/3+1);
                                for(Name hostname: allHostnames) aRecords.put(hostname, null);

                                while(true) {
                                    synchronized(threadLock) {
                                        if(currentThread!=thread) break;
                                    }
                                    try {
                                        long startTime = System.currentTimeMillis();

                                        // Query all nameservers for all involved dns entries in parallel, getting all A records
                                        // Add any errors or warnings to the lists and return null if unable to get A records.
                                        for(final Name nameserver : allNameservers) {
                                            final SimpleResolver resolver = getSimpleResolver(nameserver);
                                            Map<Name,Future<String[]>> hostnameFutures = futures.get(nameserver);
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
                                                                            newErrors.add(ApplicationResources.accessor.getMessage("DnsMonitor.lookup.HOST_NOT_FOUND", nameserver, hostname));
                                                                            return null;
                                                                        }
                                                                        String[] addresses = new String[records.length];
                                                                        for(int c=0;c<records.length;c++) {
                                                                            ARecord aRecord = (ARecord)records[c];
                                                                            // Verify masterDomain TTL settings match expected values, issue as a warning
                                                                            if(masterRecords.contains(hostname)) {
                                                                                long ttl = aRecord.getTTL();
                                                                                if(ttl!=masterRecordsTtl) newWarnings.add(ApplicationResources.accessor.getMessage("DnsMonitor.lookup.unexpectedTtl", nameserver, hostname, masterRecordsTtl, ttl));
                                                                            }
                                                                            addresses[c] = aRecord.getAddress().getHostAddress();
                                                                        }
                                                                        // Sort all addresses returned, so we may easily compare for equality
                                                                        if(addresses.length>1) Arrays.sort(addresses);
                                                                        return addresses;
                                                                    case Lookup.UNRECOVERABLE :
                                                                        newErrors.add(ApplicationResources.accessor.getMessage("DnsMonitor.lookup.UNRECOVERABLE", nameserver, hostname));
                                                                        return null;
                                                                    case Lookup.TRY_AGAIN :
                                                                        newWarnings.add(ApplicationResources.accessor.getMessage("DnsMonitor.lookup.TRY_AGAIN", nameserver, hostname));
                                                                        return null;
                                                                    case Lookup.HOST_NOT_FOUND :
                                                                        newErrors.add(ApplicationResources.accessor.getMessage("DnsMonitor.lookup.HOST_NOT_FOUND", nameserver, hostname));
                                                                        return null;
                                                                    case Lookup.TYPE_NOT_FOUND :
                                                                        newErrors.add(ApplicationResources.accessor.getMessage("DnsMonitor.lookup.TYPE_NOT_FOUND", nameserver, hostname));
                                                                        return null;
                                                                    default :
                                                                        newErrors.add(ApplicationResources.accessor.getMessage("DnsMonitor.lookup.unexpectedResultCode", nameserver, hostname, result));
                                                                        return null;
                                                                }
                                                            }
                                                        }
                                                    )
                                                );
                                            }
                                        }

                                        // Get all the results, ensuring consistency between multiple results.
                                        // If any single result is inconsistent (not exactly the same set of A records), set inconsistent status and message.
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
                                                                    "DnsMonitor.aRecords.inconsistent",
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
                                                    synchronized(threadLock) {
                                                        needsLogged = currentThread==thread;
                                                    }
                                                    if(needsLogged) {
                                                        logger.log(Level.WARNING, null, exc);
                                                        newWarnings.add(exc.toString());
                                                    }
                                                }
                                            }
                                        }

                                        // Log query time
                                        long endTime = System.currentTimeMillis();

                                        // Make sure we got at least one response for every master and check multi-master support
                                        if(newDnsStatus==null) {
                                            Name firstRecord = null;
                                            String[] firstAddresses = null;
                                            for(Name masterRecord : masterRecords) {
                                                String[] addresses = aRecords.get(masterRecord);
                                                if(addresses==null || addresses.length==0) {
                                                    newDnsStatus = DnsStatus.INCONSISTENT;
                                                    newDnsStatusMessage = ApplicationResources.accessor.getMessage("DnsMonitor.masterRecord.missing", masterRecord);
                                                    break;
                                                }
                                                // Check for multi-master violation
                                                if(addresses.length>1 && !allowMultiMaster) {
                                                    newDnsStatus = DnsStatus.INCONSISTENT;
                                                    newDnsStatusMessage = ApplicationResources.accessor.getMessage("DnsMonitor.masterRecord.multiMasterNotAllowed", masterRecord, StringUtility.buildList(addresses));
                                                    break;
                                                }
                                                // All multi-record masters must have the same IP address(es) within a single node (like for domain aliases)
                                                if(firstRecord==null) {
                                                    firstRecord = masterRecord;
                                                    firstAddresses = addresses;
                                                } else if(!Arrays.equals(firstAddresses, addresses)) {
                                                    newDnsStatus = DnsStatus.INCONSISTENT;
                                                    newDnsStatusMessage = ApplicationResources.accessor.getMessage(
                                                        "DnsMonitor.multiRecordMaster.mismatch",
                                                        firstRecord,
                                                        StringUtility.buildList(firstAddresses),
                                                        masterRecord,
                                                        StringUtility.buildList(addresses)
                                                    );
                                                    break;
                                                }
                                            }
                                        }

                                        // Make sure we got one and only one response for every node
                                        Map<String,Name> nodeAddresses = new HashMap<String,Name>(); // Will be incomplete when newDnsStatus is set
                                        if(newDnsStatus==null) {

                                            RN_LOOP:
                                            for(ResourceNode<R,RN> resourceNode : resourceNodes) {
                                                if(resourceNode.getNode().isEnabled()) {
                                                    Name firstRecord = null;
                                                    String[] firstAddresses = null;
                                                    for(Name nodeRecord : resourceNode.getNodeRecords()) {
                                                        String[] addresses = aRecords.get(nodeRecord);
                                                        if(addresses==null || addresses.length==0) {
                                                            newDnsStatus = DnsStatus.INCONSISTENT;
                                                            newDnsStatusMessage = ApplicationResources.accessor.getMessage("DnsMonitor.nodeRecord.missing", nodeRecord);
                                                            break RN_LOOP;
                                                        }
                                                        // Must be only one A record
                                                        if(addresses.length>1) {
                                                            newDnsStatus = DnsStatus.INCONSISTENT;
                                                            newDnsStatusMessage = ApplicationResources.accessor.getMessage("DnsMonitor.nodeRecord.onlyOneAllowed", nodeRecord, StringUtility.buildList(addresses));
                                                            break RN_LOOP;
                                                        }
                                                        if(firstRecord==null) {
                                                            firstRecord = nodeRecord;
                                                            firstAddresses = addresses;
                                                            // Each node must have a different A record
                                                            Name duplicateNode = nodeAddresses.put(addresses[0], nodeRecord);
                                                            if(duplicateNode!=null) {
                                                                newDnsStatus = DnsStatus.INCONSISTENT;
                                                                newDnsStatusMessage = ApplicationResources.accessor.getMessage("DnsMonitor.nodeRecord.duplicateA", duplicateNode, nodeRecord, addresses[0]);
                                                                break RN_LOOP;
                                                            }
                                                        } else if(!Arrays.equals(firstAddresses, addresses)) {
                                                            // All multi-record nodes must have the same IP address(es) within a single node (like for domain aliases)
                                                            newDnsStatus = DnsStatus.INCONSISTENT;
                                                            newDnsStatusMessage = ApplicationResources.accessor.getMessage(
                                                                "DnsMonitor.multiRecordNode.mismatch",
                                                                firstRecord,
                                                                StringUtility.buildList(firstAddresses),
                                                                nodeRecord,
                                                                StringUtility.buildList(addresses)
                                                            );
                                                            break RN_LOOP;
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        if(newDnsStatus==null) {
                                            // Inconsistent if any master A record is outside the expected nodeDomains
                                        MASTER_LOOP :
                                            for(Name masterRecord : masterRecords) {
                                                for(String address : aRecords.get(masterRecord)) {
                                                    if(!nodeAddresses.containsKey(address)) {
                                                        newDnsStatus = DnsStatus.INCONSISTENT;
                                                        newDnsStatusMessage = ApplicationResources.accessor.getMessage(
                                                            "DnsMonitor.masterARecordDoesntMatchNode",
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

                                        if(newDnsStatus==null) newDnsStatus = DnsStatus.UNKNOWN;
                                        synchronized(threadLock) {
                                            if(currentThread!=thread) break;
                                            setDnsResult(new DnsResult(startTime, endTime, newDnsStatus, newDnsStatusMessage, newWarnings, newErrors));
                                        }
                                    } catch(RejectedExecutionException exc) {
                                        // Normal during shutdown
                                        boolean needsLogged;
                                        synchronized(threadLock) {
                                            needsLogged = currentThread==thread;
                                        }
                                        if(needsLogged) logger.log(Level.SEVERE, null, exc);
                                    } catch(Exception exc) {
                                        logger.log(Level.SEVERE, null, exc);
                                    } finally {
                                        // Reset reused objects
                                        newWarnings.clear();
                                        newErrors.clear();
                                        for(Name nameserver : allNameservers) {
                                            Map<Name,Future<String[]>> hostnameFutures = futures.get(nameserver);
                                            for(Name hostname : allHostnames) hostnameFutures.put(hostname, null); // Clear by setting to null to avoid recreating Entry objects
                                        }
                                        for(Name hostname: allHostnames) aRecords.put(hostname, null);
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
                    thread.setPriority(THREAD_PRIORITY);
                    thread.start();
                }
            }
        }
    }

    /**
     * Stops this node monitor.
     */
    void stop(boolean isReloadingConfiguration) {
        synchronized(threadLock) {
            thread = null;
            long currentTime = System.currentTimeMillis();
            setDnsResult(
                new DnsResult(
                    currentTime,
                    currentTime,
                    DnsStatus.STOPPED,
                    ApplicationResources.accessor.getMessage(
                        isReloadingConfiguration ? "DnsMonitor.stop.reloadingConfiguration.statusMessage" : "DnsMonitor.stop.statusMessage"
                    ),
                    null,
                    null
                )
            );
        }
    }
}
