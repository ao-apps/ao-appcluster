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
import org.xbill.DNS.Name;
import org.xbill.DNS.TextParseException;

/**
 * The configuration is provided in a properties file.
 *
 * @author  AO Industries, Inc.
 */
public class PropertiesConfiguration implements AppClusterConfiguration {

    private static final Logger logger = Logger.getLogger(PropertiesConfiguration.class.getName());

    private static final int THREAD_PRIORITY = Thread.NORM_PRIORITY + 1;

    /**
     * Checks once every five seconds for configuration file updates.
     */
    private static final long FILE_CHECK_INTERVAL = 5000;

    private final List<ConfigurationListener> listeners = new ArrayList<ConfigurationListener>();

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
    public void start() throws AppClusterConfigurationException {
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
                                                    for(ConfigurationListener listener : listeners) listener.onConfigurationChanged();
                                                }
                                            }
                                        } catch(Exception exc) {
                                            logger.log(Level.SEVERE, null, exc);
                                        }
                                    }
                                }
                            },
                            PropertiesConfiguration.class.getName()+".fileMonitorThread"
                        );
                        fileMonitorThread.setPriority(THREAD_PRIORITY);
                        fileMonitorThread.start();
                    }
                }
            } catch(IOException exc) {
                throw new AppClusterConfigurationException(exc);
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
    public void addConfigurationListener(ConfigurationListener listener) {
        synchronized(listeners) {
            boolean found = false;
            for(ConfigurationListener existing : listeners) {
                if(existing==listener) {
                    found = true;
                    break;
                }
            }
            if(!found) listeners.add(listener);
        }
    }

    @Override
    public void removeConfigurationListener(ConfigurationListener listener) {
        synchronized(listeners) {
            for(int i=0; i<listeners.size(); i++) {
                if(listeners.get(i)==listener) listeners.remove(i--);
            }
        }
    }

    /**
     * Gets a trimmed property value, not allowing null or empty string.
     */
    private String getString(String propertyName) throws AppClusterConfigurationException {
        String value;
        synchronized(fileMonitorLock) {
            value = properties.getProperty(propertyName);
        }
        if(value==null || (value=value.trim()).length()==0) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getString.missingValue", propertyName));
        return value;
    }

    private boolean getBoolean(String propertyName) throws AppClusterConfigurationException {
        String value = getString(propertyName);
        if("true".equals(value)) return true;
        if("false".equals(value)) return false;
        throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getBoolean.invalidValue", propertyName, value));
    }

    private int getInt(String propertyName) throws AppClusterConfigurationException {
        String value = getString(propertyName);
        try {
            return Integer.parseInt(value);
        } catch(NumberFormatException exc) {
            throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getInt.invalidValue", propertyName, value));
        }
    }

    /**
     * Gets a unique set of trimmed strings.  Must have at least one value.
     */
    private Set<String> getUniqueStrings(String propertyName) throws AppClusterConfigurationException {
        List<String> values = StringUtility.splitStringCommaSpace(getString(propertyName));
        Set<String> set = new LinkedHashSet<String>(values.size()*4/3+1);
        for(String value : values) {
            value = value.trim();
            if(value.length()>0 && !set.add(value)) {
                throw new AppClusterConfigurationException(
                    ApplicationResources.accessor.getMessage("PropertiesConfiguration.getStrings.duplicate", propertyName, value)
                );
            }
        }
        if(set.isEmpty()) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getString.missingValue", propertyName));
        return set;
    }

    /**
     * Gets a unique set of trimmed names.  Must have at least one value.
     */
    private Set<Name> getUniqueNames(String propertyName) throws AppClusterConfigurationException {
        try {
            List<String> values = StringUtility.splitStringCommaSpace(getString(propertyName));
            Set<Name> set = new LinkedHashSet<Name>(values.size()*4/3+1);
            for(String value : values) {
                value = value.trim();
                if(value.length()>0 && !set.add(Name.fromString(value))) {
                    throw new AppClusterConfigurationException(
                        ApplicationResources.accessor.getMessage("PropertiesConfiguration.getStrings.duplicate", propertyName, value)
                    );
                }
            }
            if(set.isEmpty()) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getString.missingValue", propertyName));
            return set;
        } catch(TextParseException exc) {
            throw new AppClusterConfigurationException(exc);
        }
    }

    /**
     * Gets a unique set of trimmed names.  Must have at least one value.
     */
    private Name getName(String propertyName) throws AppClusterConfigurationException {
        try {
            return Name.fromString(getString(propertyName));
        } catch(TextParseException exc) {
            throw new AppClusterConfigurationException(exc);
        }
    }

    @Override
    public boolean isEnabled() throws AppClusterConfigurationException {
        return getBoolean("appcluster.enabled");
    }

    @Override
    public String getDisplay() throws AppClusterConfigurationException {
        return getString("appcluster.display");
    }

    @Override
    public AppClusterLogger getClusterLogger() throws AppClusterConfigurationException {
        String propertyName = "appcluster.log.type";
        String logType = getString(propertyName);
        if("jdbc".equals(logType)) return new JdbcClusterLogger(getString("appcluster.log.name"));
        if("properties".equals(logType)) return new PropertiesClusterLogger(new File(getString("appcluster.log.path")));
        throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getClusterLogger.unexpectedType", propertyName, logType));
    }

    static class PropertiesNodeConfiguration implements NodeConfiguration {

        private final String id;
        private final boolean enabled;
        private final String display;
        private final Name hostname;
        private final Set<Name> nameservers;

        PropertiesNodeConfiguration(String id, boolean enabled, String display, Name hostname, Collection<Name> nameservers) {
            this.id = id;
            this.enabled = enabled;
            this.display = display;
            this.hostname = hostname;
            this.nameservers = Collections.unmodifiableSet(new LinkedHashSet<Name>(nameservers));
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
        public Name getHostname() {
            return hostname;
        }

        @Override
        public Set<Name> getNameservers() {
            return nameservers;
        }
    }

    @Override
    public Set<NodeConfiguration> getNodeConfigurations() throws AppClusterConfigurationException {
        Set<String> ids = getUniqueStrings("appcluster.nodes");
        Set<NodeConfiguration> nodes = new LinkedHashSet<NodeConfiguration>(ids.size()*4/3+1);
        for(String id : ids) {
            if(
                !nodes.add(
                    new PropertiesNodeConfiguration(
                        id,
                        getBoolean("appcluster.node."+id+".enabled"),
                        getString("appcluster.node."+id+".display"),
                        getName("appcluster.node."+id+".hostname"),
                        getUniqueNames("appcluster.node."+id+".nameservers")
                    )
                )
            ) throw new AssertionError();
        }
        return Collections.unmodifiableSet(nodes);
    }

    abstract static class PropertiesResourceNodeConfiguration implements ResourceNodeConfiguration {

        private final String resourceId;
        private final String nodeId;
        private final Set<Name> slaveRecords;

        PropertiesResourceNodeConfiguration(String resourceId, String nodeId, Collection<Name> slaveRecords) {
            this.resourceId = resourceId;
            this.nodeId = nodeId;
            this.slaveRecords = Collections.unmodifiableSet(new LinkedHashSet<Name>(slaveRecords));
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
        public Set<Name> getSlaveRecords() {
            return slaveRecords;
        }
    }

    static class PropertiesRsyncResourceNodeConfiguration extends PropertiesResourceNodeConfiguration implements RsyncResourceNodeConfiguration {

        private final String username;
        private final String path;
        private final String backupDir;
        private final int backupDays;

        PropertiesRsyncResourceNodeConfiguration(String resourceId, String nodeId, Collection<Name> slaveRecords, String username, String path, String backupDir, int backupDays) {
            super(resourceId, nodeId, slaveRecords);
            this.username = username;
            this.path = path;
            this.backupDir = backupDir;
            this.backupDays = backupDays;
        }

        @Override
        public String getUsername() {
            return username;
        }

        @Override
        public String getPath() {
            return path;
        }

        @Override
        public String getBackupDir() {
            return backupDir;
        }

        @Override
        public int getBackupDays() {
            return backupDays;
        }
    }

    abstract class PropertiesResourceConfiguration implements ResourceConfiguration {

        private final String id;
        private final boolean enabled;
        private final String display;
        private final boolean allowMultiMaster;
        private final Set<Name> masterRecords;
        private final int masterRecordsTtl;

        PropertiesResourceConfiguration(String id, boolean enabled, String display, boolean allowMultiMaster, Collection<Name> masterRecords, int masterRecordsTtl) {
            this.id = id;
            this.enabled = enabled;
            this.display = display;
            this.allowMultiMaster = allowMultiMaster;
            this.masterRecords = Collections.unmodifiableSet(new LinkedHashSet<Name>(masterRecords));
            this.masterRecordsTtl = masterRecordsTtl;
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
        public boolean getAllowMultiMaster() {
            return allowMultiMaster;
        }

        @Override
        public Set<Name> getMasterRecords() {
            return masterRecords;
        }

        @Override
        public int getMasterRecordsTtl() {
            return masterRecordsTtl;
        }

        @Override
        abstract public Set<? extends ResourceNodeConfiguration> getResourceNodeConfigurations() throws AppClusterConfigurationException;
    }

    class RsyncPropertiesResourceConfiguration extends PropertiesResourceConfiguration implements RsyncResourceConfiguration {

        private final boolean delete;

        RsyncPropertiesResourceConfiguration(String id, boolean enabled, String display, boolean allowMultiMaster, Collection<Name> masterRecords, int masterRecordsTtl, boolean delete) {
            super(id, enabled, display, allowMultiMaster, masterRecords, masterRecordsTtl);
            this.delete = delete;
        }

        @Override
        public boolean isDelete() {
            return delete;
        }

        @Override
        public Set<RsyncResourceNodeConfiguration> getResourceNodeConfigurations() throws AppClusterConfigurationException {
            String resourceId = getId();
            Set<String> nodeIds = getUniqueStrings("appcluster.resource."+resourceId+".nodes");
            Set<RsyncResourceNodeConfiguration> resourceNodes = new LinkedHashSet<RsyncResourceNodeConfiguration>(nodeIds.size()*4/3+1);
            for(String nodeId : nodeIds) {
                if(
                    !resourceNodes.add(
                        new PropertiesRsyncResourceNodeConfiguration(
                            resourceId,
                            nodeId,
                            getUniqueNames("appcluster.resource."+resourceId+".node."+nodeId+".slaveRecords"),
                            getString("appcluster.resource."+resourceId+".node."+nodeId+".username"),
                            getString("appcluster.resource."+resourceId+".node."+nodeId+".path"),
                            getString("appcluster.resource."+resourceId+".node."+nodeId+".backupDir"),
                            getInt("appcluster.resource."+resourceId+".node."+nodeId+".backupDays")
                        )
                    )
                ) throw new AssertionError();
            }
            return Collections.unmodifiableSet(resourceNodes);
        }
    }

    @Override
    public Set<ResourceConfiguration> getResourceConfigurations() throws AppClusterConfigurationException {
        Set<String> ids = getUniqueStrings("appcluster.resources");
        Set<ResourceConfiguration> resources = new LinkedHashSet<ResourceConfiguration>(ids.size()*4/3+1);
        for(String id : ids) {
            String propertyName = "appcluster.resource."+id+".type";
            String resourceType = getString(propertyName);
            if("rsync".equals(resourceType)) {
                if(
                    !resources.add(
                        new RsyncPropertiesResourceConfiguration(
                            id,
                            getBoolean("appcluster.resource."+id+".enabled"),
                            getString("appcluster.resource."+id+".display"),
                            getBoolean("appcluster.resource."+id+".allowMultiMaster"),
                            getUniqueNames("appcluster.resource."+id+".masterRecords"),
                            getInt("appcluster.resource."+id+".masterRecordsTtl"),
                            getBoolean("appcluster.resource."+id+".delete")
                        )
                    )
                ) throw new AssertionError();
            } else {
                throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("PropertiesConfiguration.getResourceConfigurations.unexpectedType", propertyName, resourceType));
            }
        }
        return Collections.unmodifiableSet(resources);
    }
}