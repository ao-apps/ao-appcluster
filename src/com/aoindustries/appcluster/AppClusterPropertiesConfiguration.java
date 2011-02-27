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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
public class AppClusterPropertiesConfiguration implements AppClusterConfiguration {

    private static final Logger logger = Logger.getLogger(AppClusterPropertiesConfiguration.class.getName());

    private static final int THREAD_PRIORITY = Thread.NORM_PRIORITY + 1;

    /**
     * Checks once every five seconds for configuration file updates.
     */
    private static final long FILE_CHECK_INTERVAL = 5000;

    private final List<AppClusterConfigurationListener> listeners = new ArrayList<AppClusterConfigurationListener>();

    private final File file;

    private final Object fileMonitorLock = new Object();
    private Thread fileMonitorThread; // All access uses fileMonitorLock
    private long fileLastModified; // All access uses fileMonitorLock
    private Properties properties; // All access uses fileMonitorLock

    /**
     * Loads the properties from the provided file.  Will detect changes in the
     * file based on modified time, checking once every FILE_CHECK_INTERVAL milliseconds.
     */
    public AppClusterPropertiesConfiguration(File file) {
        this.file = file;
        this.properties = null;
    }

    /**
     * Uses the provided configuration.  No changes to the properties will be detected.
     */
    public AppClusterPropertiesConfiguration(Properties properties) {
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
                                                    AppClusterPropertiesConfiguration.this.properties = newProperties;
                                                    notifyListeners = true;
                                                }
                                            }
                                            if(notifyListeners) {
                                                synchronized(listeners) {
                                                    for(AppClusterConfigurationListener listener : listeners) {
                                                        try {
                                                            listener.onConfigurationChanged();
                                                        } catch(Exception exc) {
                                                            logger.log(Level.SEVERE, null, exc);
                                                        }
                                                    }
                                                }
                                            }
                                        } catch(Exception exc) {
                                            logger.log(Level.SEVERE, null, exc);
                                        }
                                    }
                                }
                            },
                            AppClusterPropertiesConfiguration.class.getName()+".fileMonitorThread"
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
    public void addConfigurationListener(AppClusterConfigurationListener listener) {
        synchronized(listeners) {
            boolean found = false;
            for(AppClusterConfigurationListener existing : listeners) {
                if(existing==listener) {
                    found = true;
                    break;
                }
            }
            if(!found) listeners.add(listener);
        }
    }

    @Override
    public void removeConfigurationListener(AppClusterConfigurationListener listener) {
        synchronized(listeners) {
            for(int i=0; i<listeners.size(); i++) {
                if(listeners.get(i)==listener) listeners.remove(i--);
            }
        }
    }

    /**
     * Gets a trimmed property value, not allowing null or empty string.
     */
    public String getString(String propertyName) throws AppClusterConfigurationException {
        String value;
        synchronized(fileMonitorLock) {
            value = properties.getProperty(propertyName);
        }
        if(value==null || (value=value.trim()).length()==0) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppClusterPropertiesConfiguration.getString.missingValue", propertyName));
        return value;
    }

    public boolean getBoolean(String propertyName) throws AppClusterConfigurationException {
        String value = getString(propertyName);
        if("true".equals(value)) return true;
        if("false".equals(value)) return false;
        throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppClusterPropertiesConfiguration.getBoolean.invalidValue", propertyName, value));
    }

    public int getInt(String propertyName) throws AppClusterConfigurationException {
        String value = getString(propertyName);
        try {
            return Integer.parseInt(value);
        } catch(NumberFormatException exc) {
            throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppClusterPropertiesConfiguration.getInt.invalidValue", propertyName, value));
        }
    }

    public Name getName(String propertyName) throws AppClusterConfigurationException {
        try {
            return Name.fromString(getString(propertyName));
        } catch(TextParseException exc) {
            throw new AppClusterConfigurationException(exc);
        }
    }

    /**
     * Gets a unique set of trimmed strings.  Must have at least one value.
     */
    public Set<String> getUniqueStrings(String propertyName) throws AppClusterConfigurationException {
        List<String> values = StringUtility.splitStringCommaSpace(getString(propertyName));
        Set<String> set = new LinkedHashSet<String>(values.size()*4/3+1);
        for(String value : values) {
            value = value.trim();
            if(value.length()>0 && !set.add(value)) {
                throw new AppClusterConfigurationException(
                    ApplicationResources.accessor.getMessage("AppClusterPropertiesConfiguration.getStrings.duplicate", propertyName, value)
                );
            }
        }
        if(set.isEmpty()) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppClusterPropertiesConfiguration.getString.missingValue", propertyName));
        return Collections.unmodifiableSet(set);
    }

    /**
     * Gets a unique set of trimmed names.  Must have at least one value.
     */
    public Set<? extends Name> getUniqueNames(String propertyName) throws AppClusterConfigurationException {
        try {
            List<String> values = StringUtility.splitStringCommaSpace(getString(propertyName));
            Set<Name> set = new LinkedHashSet<Name>(values.size()*4/3+1);
            for(String value : values) {
                value = value.trim();
                if(value.length()>0 && !set.add(Name.fromString(value))) {
                    throw new AppClusterConfigurationException(
                        ApplicationResources.accessor.getMessage("AppClusterPropertiesConfiguration.getStrings.duplicate", propertyName, value)
                    );
                }
            }
            if(set.isEmpty()) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppClusterPropertiesConfiguration.getString.missingValue", propertyName));
            return Collections.unmodifiableSet(set);
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
        throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppClusterPropertiesConfiguration.getClusterLogger.unexpectedType", propertyName, logType));
    }

    @Override
    public Set<? extends NodePropertiesConfiguration> getNodeConfigurations() throws AppClusterConfigurationException {
        Set<String> ids = getUniqueStrings("appcluster.nodes");
        Set<NodePropertiesConfiguration> nodes = new LinkedHashSet<NodePropertiesConfiguration>(ids.size()*4/3+1);
        for(String id : ids) {
            if(
                !nodes.add(new NodePropertiesConfiguration(this, id))
            ) throw new AssertionError();
        }
        return Collections.unmodifiableSet(nodes);
    }

    private static final Map<String,ResourcePropertiesConfigurationFactory> factoryCache = new HashMap<String,ResourcePropertiesConfigurationFactory>();
    private static ResourcePropertiesConfigurationFactory getResourcePropertiesConfigurationFactory(String classname) throws AppClusterConfigurationException {
        synchronized(factoryCache) {
            ResourcePropertiesConfigurationFactory factory = factoryCache.get(classname);
            if(factory==null) {
                try {
                    factory = Class.forName(classname).asSubclass(ResourcePropertiesConfigurationFactory.class).newInstance();
                    factoryCache.put(classname, factory);
                } catch(ClassNotFoundException exc) {
                    throw new AppClusterConfigurationException(exc);
                } catch(ClassCastException exc) {
                    throw new AppClusterConfigurationException(exc);
                } catch(InstantiationException exc) {
                    throw new AppClusterConfigurationException(exc);
                } catch(IllegalAccessException exc) {
                    throw new AppClusterConfigurationException(exc);
                }
            }
            return factory;
        }
    }

    @Override
    public Set<? extends ResourceConfiguration<?,?>> getResourceConfigurations() throws AppClusterConfigurationException {
        // Get all of the resource types
        Set<String> types = getUniqueStrings("appcluster.resourceTypes");
        Map<String,ResourcePropertiesConfigurationFactory<?,?>> factories = new HashMap<String,ResourcePropertiesConfigurationFactory<?,?>>(types.size()*4/3+1);
        for(String type : types) {
            factories.put(type, getResourcePropertiesConfigurationFactory(getString("appcluster.resourceType."+type+".factory")));
        }
        Set<String> ids = getUniqueStrings("appcluster.resources");
        Set<ResourceConfiguration<?,?>> resources = new LinkedHashSet<ResourceConfiguration<?,?>>(ids.size()*4/3+1);
        for(String id : ids) {
            String propertyName = "appcluster.resource."+id+".type";
            String type = getString(propertyName);
            ResourcePropertiesConfigurationFactory factory = factories.get(type);
            if(factory==null) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppClusterPropertiesConfiguration.getResourceConfigurations.unexpectedType", propertyName, type));
            if(!resources.add(factory.newResourcePropertiesConfiguration(this, id))) throw new AssertionError();
        }
        return Collections.unmodifiableSet(resources);
    }
}
