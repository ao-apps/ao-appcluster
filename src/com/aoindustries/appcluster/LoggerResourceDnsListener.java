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

import com.aoindustries.util.Collections;
import com.aoindustries.util.StringUtility;
import java.util.Map;
import java.util.SortedSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xbill.DNS.Name;

/**
 * Logs all changes of DNS state to the logger.
 *
 * @author  AO Industries, Inc.
 */
public class LoggerResourceDnsListener implements ResourceDnsListener {

    private static final Logger logger = Logger.getLogger(LoggerResourceDnsListener.class.getName());

    private static final String eol = System.getProperty("line.separator");

    /**
     * Appends to a StringBuilder, creating it if necessary, adding a newline to separate the new message.
     */
    private static void appendWithNewline(StringBuilder message, String line) {
        if(message.length()>0) message.append(eol);
        message.append(line);
    }

    @Override
    public void onResourceDnsResult(ResourceDnsResult oldResult, ResourceDnsResult newResult) {
        Resource<?,?> resource = oldResult.getResource();

        // Log any changes, except continual changes to time
        if(logger.isLoggable(Level.FINE)) {
            logger.fine(ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.timeMillis", resource.getCluster(), resource, newResult.getEndTime() - newResult.getStartTime()));
        }
        StringBuilder infoMessages = new StringBuilder();
        StringBuilder warningMessages = new StringBuilder();
        StringBuilder errorMessages = new StringBuilder();
        // Log any master DNS record change
        {
            Map<Name,Map<Name,DnsLookupResult>> newMasterLookupResults = newResult.getMasterRecordLookups();
            Map<Name,Map<Name,DnsLookupResult>> oldMasterLookupResults = oldResult.getMasterRecordLookups();
            if(newMasterLookupResults!=null) {
                for(Name masterRecord : resource.getMasterRecords()) {
                    Map<Name,DnsLookupResult> newMasterLookups = newMasterLookupResults.get(masterRecord);
                    Map<Name,DnsLookupResult> oldMasterLookups = oldMasterLookupResults==null ? null : oldMasterLookupResults.get(masterRecord);
                    for(Name enabledNameserver : resource.getEnabledNameservers()) {
                        DnsLookupResult newDnsLookupResult = newMasterLookups.get(enabledNameserver);
                        DnsLookupResult oldDnsLookupResult = oldMasterLookups==null ? null : oldMasterLookups.get(enabledNameserver);
                        if(logger.isLoggable(Level.INFO)) {
                            SortedSet<String> newAddresses = newDnsLookupResult.getAddresses();
                            SortedSet<String> oldAddresses = oldDnsLookupResult==null ? null : oldDnsLookupResult.getAddresses();
                            if(oldAddresses==null) oldAddresses = Collections.emptySortedSet();
                            if(!newAddresses.equals(oldAddresses)) {
                                appendWithNewline(
                                    infoMessages,
                                    ApplicationResources.accessor.getMessage(
                                        "LoggingResourceDnsMonitor.onResourceDnsResult.masterRecordLookupResultChanged",
                                        resource.getCluster(),
                                        resource,
                                        masterRecord,
                                        enabledNameserver,
                                        oldAddresses==null ? "" : StringUtility.buildList(oldAddresses),
                                        StringUtility.buildList(newAddresses)
                                    )
                                );
                            }
                        }
                        if(logger.isLoggable(Level.WARNING)) {
                            SortedSet<String> newWarnings = newDnsLookupResult.getWarnings();
                            SortedSet<String> oldWarnings = oldDnsLookupResult==null ? null : oldDnsLookupResult.getWarnings();
                            if(oldWarnings==null) oldWarnings = Collections.emptySortedSet();
                            if(!newWarnings.equals(oldWarnings)) {
                                for(String warning : newWarnings) {
                                    appendWithNewline(
                                        warningMessages,
                                        ApplicationResources.accessor.getMessage(
                                            "LoggingResourceDnsMonitor.onResourceDnsResult.masterRecord.warning",
                                            resource.getCluster(),
                                            resource,
                                            masterRecord,
                                            enabledNameserver,
                                            warning
                                        )
                                    );
                                }
                            }
                        }
                        if(logger.isLoggable(Level.SEVERE)) {
                            SortedSet<String> newErrors = newDnsLookupResult.getErrors();
                            SortedSet<String> oldErrors = oldDnsLookupResult==null ? null : oldDnsLookupResult.getErrors();
                            if(oldErrors==null) oldErrors = Collections.emptySortedSet();
                            if(!newErrors.equals(oldErrors)) {
                                for(String error : newErrors) {
                                    appendWithNewline(
                                        errorMessages,
                                        ApplicationResources.accessor.getMessage(
                                            "LoggingResourceDnsMonitor.onResourceDnsResult.masterRecord.error",
                                            resource.getCluster(),
                                            resource,
                                            masterRecord,
                                            enabledNameserver,
                                            error
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        if(logger.isLoggable(Level.INFO)) {
            if(newResult.getMasterStatus()!=oldResult.getMasterStatus()) {
                appendWithNewline(infoMessages, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.masterStatusChanged", resource.getCluster(), resource, oldResult.getMasterStatus(), newResult.getMasterStatus()));
            }
            if(!newResult.getMasterStatusMessages().equals(oldResult.getMasterStatusMessages())) {
                for(String masterStatusMessage : newResult.getMasterStatusMessages()) {
                    appendWithNewline(infoMessages, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.masterStatusMessage", resource.getCluster(), resource, masterStatusMessage));
                }
            }
        }
        for(ResourceNode<?,?> resourceNode : resource.getResourceNodes().values()) {
            Node node = resourceNode.getNode();
            ResourceNodeDnsResult newNodeResult = newResult.getNodeResults().get(node);
            ResourceNodeDnsResult oldNodeResult = oldResult.getNodeResults().get(node);
            // Log any node DNS record change
            {
                Map<Name,Map<Name,DnsLookupResult>> newNodeLookupResults = newNodeResult.getNodeRecordLookups();
                Map<Name,Map<Name,DnsLookupResult>> oldNodeLookupResults = oldNodeResult.getNodeRecordLookups();
                if(newNodeLookupResults!=null) {
                    for(Name nodeRecord : resourceNode.getNodeRecords()) {
                        Map<Name,DnsLookupResult> newNodeLookups = newNodeLookupResults.get(nodeRecord);
                        Map<Name,DnsLookupResult> oldNodeLookups = oldNodeLookupResults==null ? null : oldNodeLookupResults.get(nodeRecord);
                        for(Name enabledNameserver : resource.getEnabledNameservers()) {
                            DnsLookupResult newDnsLookupResult = newNodeLookups.get(enabledNameserver);
                            DnsLookupResult oldDnsLookupResult = oldNodeLookups==null ? null : oldNodeLookups.get(enabledNameserver);
                            if(logger.isLoggable(Level.INFO)) {
                                SortedSet<String> newAddresses = newDnsLookupResult.getAddresses();
                                SortedSet<String> oldAddresses = oldDnsLookupResult==null ? null : oldDnsLookupResult.getAddresses();
                                if(oldAddresses==null) oldAddresses = Collections.emptySortedSet();
                                if(!newAddresses.equals(oldAddresses)) {
                                    appendWithNewline(
                                        infoMessages,
                                        ApplicationResources.accessor.getMessage(
                                            "LoggingResourceDnsMonitor.onResourceDnsResult.nodeRecordLookupResultChanged",
                                            resource.getCluster(),
                                            resource,
                                            node,
                                            nodeRecord,
                                            enabledNameserver,
                                            oldAddresses==null ? "" : StringUtility.buildList(oldAddresses),
                                            StringUtility.buildList(newAddresses)
                                        )
                                    );
                                }
                            }
                            if(logger.isLoggable(Level.WARNING)) {
                                SortedSet<String> newWarnings = newDnsLookupResult.getWarnings();
                                SortedSet<String> oldWarnings = oldDnsLookupResult==null ? null : oldDnsLookupResult.getWarnings();
                                if(oldWarnings==null) oldWarnings = Collections.emptySortedSet();
                                if(!newWarnings.equals(oldWarnings)) {
                                    for(String warning : newWarnings) {
                                        appendWithNewline(
                                            warningMessages,
                                            ApplicationResources.accessor.getMessage(
                                                "LoggingResourceDnsMonitor.onResourceDnsResult.nodeRecord.warning",
                                                resource.getCluster(),
                                                resource,
                                                node,
                                                nodeRecord,
                                                enabledNameserver,
                                                warning
                                            )
                                        );
                                    }
                                }
                            }
                            if(logger.isLoggable(Level.SEVERE)) {
                                SortedSet<String> newErrors = newDnsLookupResult.getErrors();
                                SortedSet<String> oldErrors = oldDnsLookupResult==null ? null : oldDnsLookupResult.getErrors();
                                if(oldErrors==null) oldErrors = Collections.emptySortedSet();
                                if(!newErrors.equals(oldErrors)) {
                                    for(String error : newErrors) {
                                        appendWithNewline(
                                            errorMessages,
                                            ApplicationResources.accessor.getMessage(
                                                "LoggingResourceDnsMonitor.onResourceDnsResult.nodeRecord.error",
                                                resource.getCluster(),
                                                resource,
                                                node,
                                                nodeRecord,
                                                enabledNameserver,
                                                error
                                            )
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if(logger.isLoggable(Level.INFO)) {
                NodeDnsStatus newNodeStatus = newNodeResult.getNodeStatus();
                NodeDnsStatus oldNodeStatus = oldNodeResult.getNodeStatus();
                if(newNodeStatus!=oldNodeStatus) {
                    appendWithNewline(infoMessages, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.nodeStatusChanged", resource.getCluster(), resource, node, oldNodeStatus, newNodeStatus));
                }
                SortedSet<String> newNodeStatusMessages = newNodeResult.getNodeStatusMessages();
                SortedSet<String> oldNodeStatusMessages = oldNodeResult.getNodeStatusMessages();
                if(!newNodeStatusMessages.equals(oldNodeStatusMessages)) {
                    for(String nodeStatusMessage : newNodeStatusMessages) {
                        appendWithNewline(infoMessages, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.nodeStatusMessage", resource.getCluster(), resource, node, nodeStatusMessage));
                    }
                }
            }
        }
        if(infoMessages.length()>0) logger.info(infoMessages.toString());
        if(warningMessages.length()>0) logger.warning(warningMessages.toString());
        if(errorMessages.length()>0) logger.severe(errorMessages.toString());
    }
}
