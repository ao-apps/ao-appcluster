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

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xbill.DNS.Name;
import org.xbill.DNS.TextParseException;

/**
 * Central AppCluster manager.
 *
 * @author  AO Industries, Inc.
 */
public class AppCluster {

    private static final Logger logger = Logger.getLogger(AppCluster.class.getName());

    private static final int EXECUTOR_THREAD_PRIORITY = Thread.NORM_PRIORITY - 1;

    private final AppClusterConfiguration configuration;

    /**
     * Started flag.
     */
    private final Object startedLock = new Object();
    private boolean started = false; // Protected by startedLock
    private boolean enabled; // Protected by startedLock
    private String display; // Protected by startedLock
    private ExecutorService executorService; // Protected by startLock
    private AppClusterLogger clusterLogger; // Protected by startedLock
    private Map<String,Node> nodes; // Protected by startedLock
    private Node thisNode; // Protected by startedLock
    private Map<String,Resource> resources; // Protected by startedLock

    /**
     * Creates a cluster with the provided configuration.
     * The cluster is not started until <code>start</code> is called.
     *
     * @see #start()
     */
    public AppCluster(AppClusterConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Creates a cluster loading configuration from the provided properties file.
     * Any change to the file will cause an automatic reload of the cluster configuration.
     * The cluster is not started until <code>start</code> is called.
     *
     * @see #start()
     */
    public AppCluster(File file) {
        this.configuration = new PropertiesConfiguration(file);
    }

    /**
     * Creates a cluster configurated from the provided properties file.
     * Changes to the properties file will not result in a cluster configuration.
     * The cluster is not started until <code>start</code> is called.
     *
     * @see #start()
     */
    public AppCluster(Properties properties) {
        this.configuration = new PropertiesConfiguration(properties);
    }

    /**
     * Performs a consistency check on a configuration.
     */
    /*
    public static void checkConfiguration(AppClusterConfiguration configuration) throws AppClusterConfiguration.AppClusterConfigurationException {
        checkConfiguration(
            configuration.getNodeConfigurations(),
            configuration.getResourceConfigurations()
        );
    }*/

    /**
     * Performs a consistency check on a configuration.
     */
    public static void checkConfiguration(Set<AppClusterConfiguration.NodeConfiguration> nodeConfigurations, Set<AppClusterConfiguration.ResourceConfiguration> resourceConfigurations) throws AppClusterConfiguration.AppClusterConfigurationException {
        // Each node must have a distinct display
        Set<String> strings = new HashSet<String>(nodeConfigurations.size()*4/3+1);
        for(AppClusterConfiguration.NodeConfiguration nodeConfiguration : nodeConfigurations) {
            String display = nodeConfiguration.getDisplay();
            if(!strings.add(display)) throw new AppClusterConfiguration.AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.duplicateNodeDisplay", display));
        }

        // Each node must have a distinct hostname
        Set<Name> names = new HashSet<Name>(nodeConfigurations.size()*4/3+1);
        for(AppClusterConfiguration.NodeConfiguration nodeConfiguration : nodeConfigurations) {
            Name hostname = nodeConfiguration.getHostname();
            if(!names.add(hostname)) throw new AppClusterConfiguration.AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.duplicateNodeHostname", hostname));
        }

        // Each node must have a distinct display
        strings.clear();
        for(AppClusterConfiguration.ResourceConfiguration resourceConfiguration : resourceConfigurations) {
            String display = resourceConfiguration.getDisplay();
            if(!strings.add(display)) throw new AppClusterConfiguration.AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.duplicateResourceDisplay", display));
        }

        // Each resource-node must have no overlap between nodeRecords and masterRecords of the resource
        for(AppClusterConfiguration.ResourceConfiguration resourceConfiguration : resourceConfigurations) {
            Set<Name> masterRecords = resourceConfiguration.getMasterRecords();
            for(AppClusterConfiguration.ResourceNodeConfiguration rnc : resourceConfiguration.getResourceNodeConfigurations()) {
                for(Name nodeRecord : rnc.getNodeRecords()) {
                    if(masterRecords.contains(nodeRecord)) {
                        throw new AppClusterConfiguration.AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.nodeMatchesMaster", nodeRecord));
                    }
                }
            }
        }

        // Each resource-node must have no overlap between nodeRecords and nodeRecords of any other resource-node of the resource
        for(AppClusterConfiguration.ResourceConfiguration resourceConfiguration : resourceConfigurations) {
            Set<? extends AppClusterConfiguration.ResourceNodeConfiguration> resourceNodeConfigurations = resourceConfiguration.getResourceNodeConfigurations();
            for(AppClusterConfiguration.ResourceNodeConfiguration rnc1 : resourceNodeConfigurations) {
                Set<Name> nodeRecords1 = rnc1.getNodeRecords();
                for(AppClusterConfiguration.ResourceNodeConfiguration rnc2 : resourceNodeConfigurations) {
                    if(!rnc1.equals(rnc2)) {
                        for(Name nodeRecord : rnc2.getNodeRecords()) {
                            if(nodeRecords1.contains(nodeRecord)) {
                                throw new AppClusterConfiguration.AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.nodeMatchesOtherNode", nodeRecord));
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * When the configuration changes, do shutdown and startUp.
     */
    private final AppClusterConfiguration.ConfigurationListener configUpdated = new AppClusterConfiguration.ConfigurationListener() {
        @Override
        public void onConfigurationChanged() {
            synchronized(startedLock) {
                if(started) {
                    try {
                        if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("AppCluster.onConfigurationChanged.info", configuration.getDisplay()));
                    } catch(AppClusterConfiguration.AppClusterConfigurationException exc) {
                        logger.log(Level.SEVERE, null, exc);
                    }
                    shutdown();
                    try {
                        startUp();
                    } catch(AppClusterConfiguration.AppClusterConfigurationException exc) {
                        logger.log(Level.SEVERE, null, exc);
                    }
                }
            }
        }
    };

    /**
     * Starts this cluster manager.
     *
     * @see #stop()
     */
    public void start() throws AppClusterConfiguration.AppClusterConfigurationException {
        synchronized(startedLock) {
            if(!started) {
                configuration.start();
                if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("AppCluster.start.info", configuration.getDisplay()));
                configuration.addConfigurationListener(configUpdated);
                started = true;
                startUp();
            }
        }
    }

    /**
     * Stops this cluster manager.
     *
     * @see #start()
     */
    public void stop() {
        synchronized(startedLock) {
            if(started) {
                try {
                    if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("AppCluster.stop.info", configuration.getDisplay()));
                } catch(AppClusterConfiguration.AppClusterConfigurationException exc) {
                    logger.log(Level.SEVERE, null, exc);
                }
                shutdown();
                started = false;
                configuration.removeConfigurationListener(configUpdated);
                configuration.stop();
            }
        }
    }

    /**
     * If the cluster is disabled, every node and resource will also be disabled.
     */
    public boolean isEnabled() throws IllegalStateException {
        synchronized(startedLock) {
            if(!started) throw new IllegalStateException();
            return enabled;
        }
    }

    /**
     * Gets the display name for this cluster.
     */
    public String getDisplay() throws IllegalStateException {
        synchronized(startedLock) {
            if(display==null) throw new IllegalStateException();
            return display;
        }
    }

    @Override
    public String toString() {
        try {
            return getDisplay();
        } catch(IllegalStateException exc) {
            logger.log(Level.WARNING, null, exc);
            return super.toString();
        }
    }

    /**
     * Gets the executor service for this cluster.
     * Only available when started.
     */
    ExecutorService getExecutorService() throws IllegalStateException {
        synchronized(startedLock) {
            if(executorService==null) throw new IllegalStateException();
            return executorService;
        }
    }

    /**
     * Only available when started.
     */
    public AppClusterLogger getClusterLogger() throws IllegalStateException {
        synchronized(startedLock) {
            if(clusterLogger==null) throw new IllegalStateException();
            return clusterLogger;
        }
    }

    /**
     * Only available when started.
     */
    public Map<String,Node> getNodes() throws IllegalStateException {
        synchronized(startedLock) {
            if(nodes==null) throw new IllegalStateException();
            return nodes;
        }
    }

    /**
     * Gets the node this machine represents or <code>null</code> if this
     * machine is not one of the nodes.
     * Only available when started.
     */
    public Node getThisNode() throws IllegalStateException {
        synchronized(startedLock) {
            if(!started) throw new IllegalStateException();
            return thisNode;
        }
    }

    /**
     * Only available when started.
     */
    public Map<String,Resource> getResources() throws IllegalStateException {
        synchronized(startedLock) {
            if(!started) throw new IllegalStateException();
            return resources;
        }
    }

    private void startUp() throws AppClusterConfiguration.AppClusterConfigurationException {
        synchronized(startedLock) {
            if(started) {
                try {
                    // Get system-local values
                    Name thisHostname = Name.fromString(InetAddress.getLocalHost().getHostName());

                    // Get the configuration values.
                    enabled = configuration.isEnabled();
                    display = configuration.getDisplay();
                    Set<AppClusterConfiguration.NodeConfiguration> nodeConfigurations = configuration.getNodeConfigurations();
                    Set<AppClusterConfiguration.ResourceConfiguration> resourceConfigurations = configuration.getResourceConfigurations();

                    // Check the configuration for consistency
                    checkConfiguration(nodeConfigurations, resourceConfigurations);

                    // Create the nodes
                    Map<String,Node> newNodes = new LinkedHashMap<String,Node>(nodeConfigurations.size()*4/3+1);
                    for(AppClusterConfiguration.NodeConfiguration nodeConfiguration : nodeConfigurations) {
                        Node node = new Node(this, nodeConfiguration);
                        newNodes.put(node.getId(), node);
                    }
                    nodes = Collections.unmodifiableMap(newNodes);

                    // Find this node
                    thisNode = null;
                    for(Node node : nodes.values()) {
                        if(node.getHostname().equals(thisHostname)) {
                            thisNode = node;
                            break;
                        }
                    }

                    // Start the executor service
                    executorService = Executors.newCachedThreadPool(
                        new ThreadFactory() {
                            @Override
                            public Thread newThread(Runnable r) {
                                Thread thread = new Thread(r, AppCluster.class.getName()+".executorService");
                                thread.setPriority(EXECUTOR_THREAD_PRIORITY);
                                return thread;
                            }
                        }
                    );

                    // Start the logger
                    clusterLogger = configuration.getClusterLogger();
                    clusterLogger.start();

                    // Start per-resource monitoring threads
                    Map<String,Resource> newResources = new LinkedHashMap<String,Resource>(resourceConfigurations.size()*4/3+1);
                    for(AppClusterConfiguration.ResourceConfiguration resourceConfiguration : resourceConfigurations) {
                        if(resourceConfiguration instanceof AppClusterConfiguration.RsyncResourceConfiguration) {
                             RsyncResource resource = new RsyncResource(this, (AppClusterConfiguration.RsyncResourceConfiguration)resourceConfiguration);
                             newResources.put(resourceConfiguration.getId(), resource);
                             resource.getDnsMonitor().start();
                        } else {
                            throw new AppClusterConfiguration.AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.startUp.unexpectedType", resourceConfiguration.getId(), resourceConfiguration.getClass().getName()));
                        }
                    }
                    resources = Collections.unmodifiableMap(newResources);
                } catch(TextParseException exc) {
                    throw new AppClusterConfiguration.AppClusterConfigurationException(exc);
                } catch(UnknownHostException exc) {
                    throw new AppClusterConfiguration.AppClusterConfigurationException(exc);
                }
            }
        }
    }

    private void shutdown() {
        synchronized(startedLock) {
            if(started) {
                // Stop per-resource monitoring threads
                if(resources!=null) {
                    for(Resource resource : resources.values()) resource.getDnsMonitor().stop();
                    resources = null;
                }

                // Stop the logger
                if(clusterLogger!=null) {
                    clusterLogger.stop();
                    clusterLogger = null;
                }

                // Stop the executor service
                if(executorService!=null) {
                    executorService.shutdown();
                    executorService = null;
                }

                // Clear the nodes
                nodes = null;
                thisNode = null;

                // Clear the configuration values.
                enabled = false;
                display = null;
            }
        }
    }
}
