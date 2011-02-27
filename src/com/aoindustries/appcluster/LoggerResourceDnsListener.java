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

    @Override
    public void onResourceDnsResult(ResourceDnsResult oldResult, ResourceDnsResult newResult) {
        Resource<?,?> resource = oldResult.getResource();

        // Log any changes, except continual changes to time
        if(logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.timeMillis", resource.getCluster(), resource, newResult.getEndTime().getTime() - newResult.getStartTime().getTime()));
        }
        // Log any master DNS record change
        Level level;
        {
            Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> newMasterLookupResults = newResult.getMasterRecordLookups();
            Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> oldMasterLookupResults = oldResult.getMasterRecordLookups();
            if(newMasterLookupResults!=null) {
                for(Name masterRecord : resource.getMasterRecords()) {
                    Map<? extends Nameserver,? extends DnsLookupResult> newMasterLookups = newMasterLookupResults.get(masterRecord);
                    Map<? extends Nameserver,? extends DnsLookupResult> oldMasterLookups = oldMasterLookupResults==null ? null : oldMasterLookupResults.get(masterRecord);
                    for(Nameserver enabledNameserver : resource.getEnabledNameservers()) {
                        DnsLookupResult newDnsLookupResult = newMasterLookups.get(enabledNameserver);
                        level = newDnsLookupResult.getStatus().getResourceStatus().getLogLevel();
                        if(logger.isLoggable(level)) {
                            DnsLookupResult oldDnsLookupResult = oldMasterLookups==null ? null : oldMasterLookups.get(enabledNameserver);
                            SortedSet<String> newAddresses = newDnsLookupResult.getAddresses();
                            SortedSet<String> oldAddresses = oldDnsLookupResult==null ? null : oldDnsLookupResult.getAddresses();
                            if(oldAddresses==null) oldAddresses = Collections.emptySortedSet();
                            if(!newAddresses.equals(oldAddresses)) {
                                logger.log(
                                    level,
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
                            SortedSet<String> newStatusMessages = newDnsLookupResult.getStatusMessages();
                            SortedSet<String> oldStatusMessages = oldDnsLookupResult==null ? null : oldDnsLookupResult.getStatusMessages();
                            if(oldStatusMessages==null) oldStatusMessages = Collections.emptySortedSet();
                            if(!newStatusMessages.equals(oldStatusMessages)) {
                                for(String statusMessage : newStatusMessages) {
                                    logger.log(
                                        level,
                                        ApplicationResources.accessor.getMessage(
                                            "LoggingResourceDnsMonitor.onResourceDnsResult.masterRecord.statusMessage",
                                            resource.getCluster(),
                                            resource,
                                            masterRecord,
                                            enabledNameserver,
                                            statusMessage
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        level = newResult.getMasterStatus().getResourceStatus().getLogLevel();
        if(logger.isLoggable(level)) {
            if(newResult.getMasterStatus()!=oldResult.getMasterStatus()) {
                logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.masterStatusChanged", resource.getCluster(), resource, oldResult.getMasterStatus(), newResult.getMasterStatus()));
            }
            if(!newResult.getMasterStatusMessages().equals(oldResult.getMasterStatusMessages())) {
                for(String masterStatusMessage : newResult.getMasterStatusMessages()) {
                    logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.masterStatusMessage", resource.getCluster(), resource, masterStatusMessage));
                }
            }
        }
        for(ResourceNode<?,?> resourceNode : resource.getResourceNodes()) {
            Node node = resourceNode.getNode();
            ResourceNodeDnsResult newNodeResult = newResult.getNodeResultMap().get(node);
            ResourceNodeDnsResult oldNodeResult = oldResult.getNodeResultMap().get(node);
            // Log any node DNS record change
            {
                Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> newNodeLookupResults = newNodeResult.getNodeRecordLookups();
                Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> oldNodeLookupResults = oldNodeResult.getNodeRecordLookups();
                if(newNodeLookupResults!=null) {
                    for(Name nodeRecord : resourceNode.getNodeRecords()) {
                        Map<? extends Nameserver,? extends DnsLookupResult> newNodeLookups = newNodeLookupResults.get(nodeRecord);
                        Map<? extends Nameserver,? extends DnsLookupResult> oldNodeLookups = oldNodeLookupResults==null ? null : oldNodeLookupResults.get(nodeRecord);
                        for(Nameserver enabledNameserver : resource.getEnabledNameservers()) {
                            DnsLookupResult newDnsLookupResult = newNodeLookups.get(enabledNameserver);
                            level = newDnsLookupResult.getStatus().getResourceStatus().getLogLevel();
                            if(logger.isLoggable(level)) {
                                DnsLookupResult oldDnsLookupResult = oldNodeLookups==null ? null : oldNodeLookups.get(enabledNameserver);
                                SortedSet<String> newAddresses = newDnsLookupResult.getAddresses();
                                SortedSet<String> oldAddresses = oldDnsLookupResult==null ? null : oldDnsLookupResult.getAddresses();
                                if(oldAddresses==null) oldAddresses = Collections.emptySortedSet();
                                if(!newAddresses.equals(oldAddresses)) {
                                    logger.log(
                                        level,
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
                                SortedSet<String> newStatusMessages = newDnsLookupResult.getStatusMessages();
                                SortedSet<String> oldStatusMessages = oldDnsLookupResult==null ? null : oldDnsLookupResult.getStatusMessages();
                                if(oldStatusMessages==null) oldStatusMessages = Collections.emptySortedSet();
                                if(!newStatusMessages.equals(oldStatusMessages)) {
                                    for(String statusMessage : newStatusMessages) {
                                        logger.log(
                                            level,
                                            ApplicationResources.accessor.getMessage(
                                                "LoggingResourceDnsMonitor.onResourceDnsResult.nodeRecord.statusMessage",
                                                resource.getCluster(),
                                                resource,
                                                node,
                                                nodeRecord,
                                                enabledNameserver,
                                                statusMessage
                                            )
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            NodeDnsStatus newNodeStatus = newNodeResult.getNodeStatus();
            level = newNodeStatus.getResourceStatus().getLogLevel();
            if(logger.isLoggable(level)) {
                NodeDnsStatus oldNodeStatus = oldNodeResult.getNodeStatus();
                if(newNodeStatus!=oldNodeStatus) {
                    logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.nodeStatusChanged", resource.getCluster(), resource, node, oldNodeStatus, newNodeStatus));
                }
                SortedSet<String> newNodeStatusMessages = newNodeResult.getNodeStatusMessages();
                SortedSet<String> oldNodeStatusMessages = oldNodeResult.getNodeStatusMessages();
                if(!newNodeStatusMessages.equals(oldNodeStatusMessages)) {
                    for(String nodeStatusMessage : newNodeStatusMessages) {
                        logger.log(level, ApplicationResources.accessor.getMessage("LoggingResourceDnsMonitor.onResourceDnsResult.nodeStatusMessage", resource.getCluster(), resource, node, nodeStatusMessage));
                    }
                }
            }
        }
    }
}
