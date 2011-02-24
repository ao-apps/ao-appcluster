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
import org.xbill.DNS.Name;

/**
 * The configuration for one AppCluster manager.
 *
 * @author  AO Industries, Inc.
 */
public interface AppClusterConfiguration {

    public static interface ConfigurationListener {
        void onConfigurationChanged();
    }

    public static class AppClusterConfigurationException extends AppClusterException {

        private static final long serialVersionUID = -6681668618314172644L;

        public AppClusterConfigurationException() {
        }

        public AppClusterConfigurationException(String message) {
            super(message);
        }

        public AppClusterConfigurationException(Throwable cause) {
            super(cause);
        }

        public AppClusterConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Called as the AppCluster starts, before any configuration values are accessed or listeners are added.
     */
    void start() throws AppClusterConfigurationException;

    /**
     * Called as the AppCluster stops, after all configuration values have been accessed and all listeners have been removed.
     */
    void stop();

    /**
     * Will be called when the configuration has changed in any way.
     */
    void addConfigurationListener(ConfigurationListener listener);

    /**
     * Removes listener of configuration changes.
     */
    void removeConfigurationListener(ConfigurationListener listener);

    /**
     * @see  AppCluster#isEnabled()
     */
    boolean isEnabled() throws AppClusterConfigurationException;

    /**
     * @see  AppCluster#getDisplay()
     */
    String getDisplay() throws AppClusterConfigurationException;

    /**
     * Gets the logger for the cluster.
     */
    AppClusterLogger getClusterLogger() throws AppClusterConfigurationException;

    public static interface NodeConfiguration {

        @Override
        String toString();

        @Override
        boolean equals(Object o);

        @Override
        int hashCode();

        /**
         * @see Node#getId()
         */
        String getId();

        /**
         * @see Node#isEnabled()
         */
        boolean isEnabled();

        /**
         * @see Node#getDisplay()
         */
        String getDisplay();

        /**
         * @see Node#getHostname()
         */
        String getHostname();

        /**
         * @see Node#getNameservers()
         */
        Set<Name> getNameservers();
    }

    /**
     * Gets the set of nodes for the cluster.
     */
    Set<NodeConfiguration> getNodeConfigurations() throws AppClusterConfigurationException;

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
         * @see ResourceNode#getSlaveRecords()
         */
        Set<Name> getSlaveRecords();
    }

    public static interface RsyncResourceNodeConfiguration extends ResourceNodeConfiguration {

        /**
         * @see RsyncResourceNode#getUsername()
         */
        String getUsername();

        /**
         * @see RsyncResourceNode#getPath()
         */
        String getPath();

        /**
         * @see RsyncResourceNode#getBackupDir()
         */
        String getBackupDir();

        /**
         * @see RsyncResourceNode#getBackupDays()
         */
        int getBackupDays();
    }

    public static interface ResourceConfiguration {

        @Override
        String toString();

        @Override
        boolean equals(Object o);

        @Override
        int hashCode();

        /**
         * @see Resource#getId()
         */
        String getId();

        /**
         * @see Resource#isEnabled()
         */
        boolean isEnabled();

        /**
         * @see Resource#getDisplay()
         */
        String getDisplay();

        /**
         * @see Resource#getMasterRecords()
         */
        Set<Name> getMasterRecords();

        /**
         * @see Resource#getAllowMultiMaster()
         */
        boolean getAllowMultiMaster();

        /**
         * Gets the source of per-node resource configurations.
         */
        Set<? extends ResourceNodeConfiguration> getResourceNodeConfigurations() throws AppClusterConfigurationException;
    }

    public static interface RsyncResourceConfiguration extends ResourceConfiguration {

        /**
         * Gets if files will be deleted during the rsync.
         */
        boolean isDelete();

        @Override
        Set<RsyncResourceNodeConfiguration> getResourceNodeConfigurations() throws AppClusterConfigurationException;
    }

    /**
     * Gets the set of resources for the cluster.
     */
    Set<ResourceConfiguration> getResourceConfigurations() throws AppClusterConfigurationException;
}
