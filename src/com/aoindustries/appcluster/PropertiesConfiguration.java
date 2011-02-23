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
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The configuration is provided in a properties file.
 *
 * @author  AO Industries, Inc.
 */
public class PropertiesConfiguration implements AppClusterConfiguration {

    private static final Logger logger = Logger.getLogger(PropertiesConfiguration.class.getName());

    /**
     * Checks once every five seconds for configuration file updates.
     */
    private static final long FILE_CHECK_INTERVAL = 5000;

    private final List<ConfigurationChangeListener> listeners = new ArrayList<ConfigurationChangeListener>();

    private final File file;

    private final Object fileMonitorLock = new Object();
    private Thread fileMonitorThread; // All access uses fileMonitorLock
    private long fileLastModified; // All access uses fileMonitorLock
    private Properties properties; // All access uses fileMonitorLock

    /**
     * Loads the properties from the provided file.  Will detect changes in the
     * file based on modified time, checking once every FILE_CHECK_INTERVAL milliseconds.
     */
    public PropertiesConfiguration(File file) {
        this.file = file;
        this.properties = null;
    }

    /**
     * Uses the provided configuration.  No changes to the properties will be detected.
     */
    public PropertiesConfiguration(Properties properties) {
        this.file = null;
        // Make defensive copy
        this.properties = new Properties();
        for(String key : properties.stringPropertyNames()) this.properties.setProperty(key, properties.getProperty(key));
    }

    @Override
    public void start() throws AppClusterException {
        if(file!=null) {
            try {
                synchronized(fileMonitorLock) {
                    if(fileMonitorThread==null) {
                        // Load initial properties
                        fileLastModified = file.lastModified();
                        Properties newProperties = new Properties();
                        InputStream in = new BufferedInputStream(new FileInputStream(file));
                        try {
                            newProperties.load(in);
                        } finally {
                            in.close();
                        }
                        this.properties = newProperties;
                        (
                            fileMonitorThread = new Thread(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        final Thread currentThread = Thread.currentThread();
                                        while(true) {
                                            try {
                                                try {
                                                    Thread.sleep(FILE_CHECK_INTERVAL);
                                                } catch(InterruptedException exc) {
                                                    logger.log(Level.WARNING, null, exc);
                                                }
                                                boolean notifyListeners = false;
                                                synchronized(fileMonitorLock) {
                                                    if(currentThread!=fileMonitorThread) break;
                                                    long newLastModified = file.lastModified();
                                                    if(newLastModified!=fileLastModified) {
                                                        // Reload the configuration
                                                        fileLastModified = newLastModified;
                                                        Properties newProperties = new Properties();
                                                        InputStream in = new BufferedInputStream(new FileInputStream(file));
                                                        try {
                                                            newProperties.load(in);
                                                        } finally {
                                                            in.close();
                                                        }
                                                        PropertiesConfiguration.this.properties = newProperties;
                                                        notifyListeners = true;
                                                    }
                                                }
                                                if(notifyListeners) {
                                                    synchronized(listeners) {
                                                        for(ConfigurationChangeListener listener : listeners) listener.onConfigurationChanged();
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
                }
            } catch(IOException exc) {
                throw new AppClusterException(exc);
            }
        }
    }

    @Override
    public void stop() {
        if(file!=null) {
            synchronized(fileMonitorLock) {
                fileMonitorThread = null;
                properties = null;
            }
        }
    }

    @Override
    public void addConfigurationChangeListener(ConfigurationChangeListener listener) {
        synchronized(listeners) {
            boolean found = false;
            for(ConfigurationChangeListener existing : listeners) {
                if(existing==listener) {
                    found = true;
                    break;
                }
            }
            if(!found) listeners.add(listener);
        }
    }

    @Override
    public void removeConfigurationChangeListener(ConfigurationChangeListener listener) {
        synchronized(listeners) {
            for(int i=0; i<listeners.size(); i++) {
                if(listeners.get(i)==listener) listeners.remove(i--);
            }
        }
    }

    /**
     * Gets a trimmed property value, not allowing null or empty string.
     */
    private String getString(String propertyName) throws IllegalArgumentException {
        String value;
        synchronized(fileMonitorLock) {
            value = properties.getProperty(propertyName);
        }
        if(value==null || (value=value.trim()).length()==0) throw new IllegalArgumentException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getString.missingValue", propertyName));
        return value;
    }

    private boolean getBoolean(String propertyName) throws IllegalArgumentException {
        String value = getString(propertyName);
        if("true".equals(value)) return true;
        if("false".equals(value)) return false;
        throw new IllegalArgumentException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getBoolean.validValue", propertyName, value));
    }

    /**
     * Gets a unique set of trimmed strings.  Must have at least one value.
     */
    private Set<String> getUniqueStrings(String propertyName) throws IllegalArgumentException {
        List<String> values = StringUtility.splitStringCommaSpace(getString("appcluster.nodes"));
        Set<String> set = new LinkedHashSet<String>(values.size()*4/3+1);
        for(String value : values) {
            value = value.trim();
            if(value.length()>0 && !set.add(value)) {
                throw new IllegalArgumentException(
                    ApplicationResources.accessor.getMessage("PropertiesConfiguration.getStrings.duplicate", propertyName, value)
                );
            }
        }
        if(set.isEmpty()) throw new IllegalArgumentException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getString.missingValue", propertyName));
        return set;
    }

    @Override
    public boolean isEnabled() {
        return getBoolean("appcluster.enabled");
    }

    @Override
    public String getDisplay() {
        return getString("appcluster.display");
    }

    @Override
    public AppClusterLogger getClusterLogger() {
        String logType = getString("appcluster.log.type");
        if("jdbc".equals(logType)) return new JdbcClusterLogger(getString("appcluster.log.name"));
        throw new IllegalArgumentException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getClusterLogger.expectedType", "appcluster.log.type", logType));
    }

    static class PropertiesNodeConfiguration implements NodeConfiguration {

        private final String id;
        private final boolean enabled;
        private final String display;
        private final String hostname;
        private final Set<String> nameservers;

        PropertiesNodeConfiguration(String id, boolean enabled, String display, String hostname, Collection<String> nameservers) {
            this.id = id;
            this.enabled = enabled;
            this.display = display;
            this.hostname = hostname;
            this.nameservers = Collections.unmodifiableSet(new LinkedHashSet<String>(nameservers));
        }

        @Override
        public String toString() {
            return display;
        }

        @Override
        public boolean equals(Object o) {
            if(!(o instanceof NodeConfiguration)) return false;
            return id.equals(((NodeConfiguration)o).getId());
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public String getDisplay() {
            return display;
        }

        @Override
        public String getHostname() {
            return hostname;
        }

        @Override
        public Set<String> getNameservers() {
            return nameservers;
        }
    }

    @Override
    public Set<NodeConfiguration> getNodeConfigurations() {
        Set<String> ids = getUniqueStrings("appcluster.nodes");
        Set<NodeConfiguration> nodes = new LinkedHashSet<NodeConfiguration>(ids.size()*4/3+1);
        for(String id : ids) {
            if(
                !nodes.add(
                    new PropertiesNodeConfiguration(
                        id,
                        getBoolean("appcluster.node."+id+".enabled"),
                        getString("appcluster.node."+id+".display"),
                        getString("appcluster.node."+id+".hostname"),
                        getUniqueStrings("appcluster.node."+id+".nameservers")
                    )
                )
            ) throw new AssertionError();
        }
        return Collections.unmodifiableSet(nodes);
    }

    static class PropertiesResourceNodeConfiguration implements ResourceNodeConfiguration {

        private final String resourceId;
        private final String nodeId;
        private final Set<String> slaveRecords;

        PropertiesResourceNodeConfiguration(String resourceId, String nodeId, Collection<String> slaveRecords) {
            this.resourceId = resourceId;
            this.nodeId = nodeId;
            this.slaveRecords = Collections.unmodifiableSet(new LinkedHashSet<String>(slaveRecords));
        }

        @Override
        public String toString() {
            return resourceId+'/'+nodeId;
        }

        @Override
        public boolean equals(Object o) {
            if(!(o instanceof ResourceNodeConfiguration)) return false;
            ResourceNodeConfiguration other = (ResourceNodeConfiguration)o;
            return
                resourceId.equals(other.getResourceId())
                && nodeId.equals(other.getNodeId())
            ;
        }

        @Override
        public int hashCode() {
            return resourceId.hashCode() * 31 + nodeId.hashCode();
        }

        @Override
        public String getResourceId() {
            return resourceId;
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }

        @Override
        public Set<String> getSlaveRecords() {
            return slaveRecords;
        }
    }

    class PropertiesResourceConfiguration implements ResourceConfiguration {

        private final String id;
        private final boolean enabled;
        private final String display;
        private final Set<String> masterRecords;
        private final String type;

        PropertiesResourceConfiguration(String id, boolean enabled, String display, Collection<String> masterRecords, String type) {
            this.id = id;
            this.enabled = enabled;
            this.display = display;
            this.masterRecords = Collections.unmodifiableSet(new LinkedHashSet<String>(masterRecords));
            this.type = type;
        }

        @Override
        public String toString() {
            return display;
        }

        @Override
        public boolean equals(Object o) {
            if(!(o instanceof ResourceConfiguration)) return false;
            return id.equals(((ResourceConfiguration)o).getId());
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public String getDisplay() {
            return display;
        }

        @Override
        public Set<String> getMasterRecords() {
            return masterRecords;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public Set<ResourceNodeConfiguration> getResourceNodeConfigurations() {
            Set<String> nodeIds = getUniqueStrings("appcluster.resource."+id+"nodes");
            Set<ResourceNodeConfiguration> resourceNodes = new LinkedHashSet<ResourceNodeConfiguration>(nodeIds.size()*4/3+1);
            for(String nodeId : nodeIds) {
                if(
                    !resourceNodes.add(
                        new PropertiesResourceNodeConfiguration(
                            id,
                            nodeId,
                            getUniqueStrings("appcluster.resource."+id+".node."+nodeId+".masterRecords")
                        )
                    )
                ) throw new AssertionError();
            }
            return Collections.unmodifiableSet(resourceNodes);
        }
    }

    @Override
    public Set<ResourceConfiguration> getResourceConfigurations() {
        Set<String> ids = getUniqueStrings("appcluster.resources");
        Set<ResourceConfiguration> resources = new LinkedHashSet<ResourceConfiguration>(ids.size()*4/3+1);
        for(String id : ids) {
            if(
                !resources.add(
                    new PropertiesResourceConfiguration(
                        id,
                        getBoolean("appcluster.resource."+id+".enabled"),
                        getString("appcluster.resource."+id+".display"),
                        getUniqueStrings("appcluster.resource."+id+".masterRecords"),
                        getString("appcluster.resource."+id+".type")
                    )
                )
            ) throw new AssertionError();
        }
        return Collections.unmodifiableSet(resources);
    }
}
