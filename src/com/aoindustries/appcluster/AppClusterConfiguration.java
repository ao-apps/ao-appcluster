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

import java.util.Set;

/**
 * The configuration for one AppCluster manager.
 *
 * @author  AO Industries, Inc.
 */
public interface AppClusterConfiguration {

    public static interface ConfigurationChangeListener {
        void onConfigurationChanged();
    }

    /**
     * Called as the AppCluster starts, before any configuration values are accessed or listeners are added.
     */
    void start() throws AppClusterException;

    /**
     * Called as the AppCluster stops, after all configuration values have been accessed and all listeners have been removed.
     */
    void stop();

    /**
     * Will be called when the configuration has changed in any way.
     */
    void addConfigurationChangeListener(ConfigurationChangeListener listener);

    /**
     * Removes listener of configuration changes.
     */
    void removeConfigurationChangeListener(ConfigurationChangeListener listener);

    /**
     * @see  AppCluster#isEnabled()
     */
    boolean isEnabled();

    /**
     * @see  AppCluster#getDisplay()
     */
    String getDisplay();

    /**
     * Gets the logger for the cluster.
     */
    AppClusterLogger getClusterLogger();

    public static interface NodeConfiguration {

        @Override
        String toString();

        @Override
        boolean equals(Object o);

        @Override
        int hashCode();

        /**
         * @see NodeMonitor#getId()
         */
        String getId();

        /**
         * @see NodeMonitor#isEnabled()
         */
        boolean isEnabled();

        /**
         * @see NodeMonitor#getDisplay()
         */
        String getDisplay();

        /**
         * @see NodeMonitor#getHostname()
         */
        String getHostname();

        /**
         * @see NodeMonitor#getNameservers()
         */
        Set<String> getNameservers();
    }

    /**
     * Gets the set of nodes for the cluster.
     */
    Set<NodeConfiguration> getNodeConfigurations();

    public static interface ResourceNodeConfiguration {

        @Override
        String toString();

        @Override
        boolean equals(Object o);

        @Override
        int hashCode();

        /**
         * Gets the unique ID of the resource this configuration represents.
         */
        String getResourceId();

        /**
         * Gets the unique ID of the node this configuration represents.
         */
        String getNodeId();

        /**
         * Gets the set of slave DNS records that must all the the same and
         * match the resource's masterRecords for this node to be considered
         * master.
         */
        Set<String> getSlaveRecords();

        // TODO: type-specific configuration
    }

    public static interface ResourceConfiguration {

        @Override
        String toString();

        @Override
        boolean equals(Object o);

        @Override
        int hashCode();

        /**
         * The unique ID of this resource.
         */
        String getId();

        /**
         * Determines if this resource is enabled.
         */
        boolean isEnabled();

        /**
         * Gets the display name of this resource.
         */
        String getDisplay();

        /**
         * Gets the set of master records that must all by the same.
         * The master node is determined by matching these records against
         * the resource node configuration's slave records.
         */
        Set<String> getMasterRecords();

        /**
         * Gets the type of synchronization to be performed.
         */
        String getType();

        /**
         * Gets the source of per-node resource configurations.
         */
        Set<ResourceNodeConfiguration> getResourceNodeConfigurations();
    }

    /**
     * Gets the set of resources for the cluster.
     */
    Set<ResourceConfiguration> getResourceConfigurations();
}
