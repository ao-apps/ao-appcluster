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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Central AppCluster manager.
 *
 * @author  AO Industries, Inc.
 */
public class AppCluster {

    private static final Logger logger = Logger.getLogger(AppCluster.class.getName());

    private final AppClusterConfiguration configuration;

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
    public static void checkConfiguration(AppClusterConfiguration configuration) throws IllegalArgumentException {
        // TODO: Each node must have a distinct display
        // TODO: Each node must have a distinct hostname
        // TODO: Each resource must have a distinct display
        // TODO: Each resource-node must have slaveRecords != masterRecords
        // TODO: Each resource-node must have distinct slaveRecords
        // TODO: Each resource-node path must not end in slash (/)
        // TODO: Each resource-node backupDir must not end in slash (/)
        // TODO: This host must be one of the nodes
    }

    /**
     * When the configuration changes, do shutdown and startUp.
     */
    private final AppClusterConfiguration.ConfigurationChangeListener configUpdated = new AppClusterConfiguration.ConfigurationChangeListener() {
        @Override
        public void onConfigurationChanged() {
            synchronized(startedLock) {
                if(started) {
                    if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("AppCluster.configUpdated.message", configuration.getDisplay()));
                    shutdown();
                    startUp();
                }
            }
        }
    };

    private final Object startedLock = new Object();
    private boolean started = false;

    /**
     * Starts this cluster manager.
     *
     * @see #stop()
     */
    public void start() throws AppClusterException {
        synchronized(startedLock) {
            if(!started) {
                configuration.start();
                if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("AppCluster.start.message", configuration.getDisplay()));
                configuration.addConfigurationChangeListener(configUpdated);
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
                if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("AppCluster.stop.message", configuration.getDisplay()));
                shutdown();
                started = false;
                configuration.removeConfigurationChangeListener(configUpdated);
                configuration.stop();
            }
        }
    }

    /*
     * These configuration values represent what exists at the time of last startUp.
     */
    private boolean enabled;
    private String display;

    /**
     * If the cluster is disabled, every node and resource will also be disabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Gets the display name for this cluster.
     */
    public String getDisplay() {
        return display;
    }

    @Override
    public String toString() {
        return display;
    }

    /*
     * These are the start/stop components.
     */
    private AppClusterLogger clusterLogger;
    private Map<String,NodeMonitor> nodeMonitors;

    private void startUp() {
        synchronized(startedLock) {
            if(started) {
                // Check the configuration for consistency
                checkConfiguration(configuration);

                // Get the configuration values.
                enabled = configuration.isEnabled();
                display = configuration.getDisplay();
                Set<AppClusterConfiguration.NodeConfiguration> nodeConfigurations = configuration.getNodeConfigurations();

                // Start the logger
                clusterLogger = configuration.getClusterLogger();
                clusterLogger.start();

                // Start per-node monitoring thread
                nodeMonitors = new LinkedHashMap<String,NodeMonitor>(nodeConfigurations.size()*4/3+1);
                for(AppClusterConfiguration.NodeConfiguration nodeConfiguration : nodeConfigurations) {
                    NodeMonitor nodeMonitor = new NodeMonitor(this, nodeConfiguration);
                    nodeMonitors.put(nodeConfiguration.getId(), nodeMonitor);
                    nodeMonitor.start();
                }

                // TODO: Start per-resource monitoring thread
            }
        }
    }

    private void shutdown() {
        synchronized(startedLock) {
            if(started) {
                // TODO: Stop per-resource monitoring thread

                // Stop per-node monitoring threads
                if(nodeMonitors!=null) {
                    for(NodeMonitor nodeMonitor : nodeMonitors.values()) nodeMonitor.stop();
                    nodeMonitors = null;
                }

                // Stop the logger
                if(clusterLogger!=null) {
                    clusterLogger.stop();
                    clusterLogger = null;
                }

                // Clear the configuration values.
                enabled = false;
                display = null;
            }
        }
    }
}
