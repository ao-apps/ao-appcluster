/*
 * ao-appcluster - Application-level clustering tools.
 * Copyright (C) 2011, 2015, 2016  AO Industries, Inc.
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

import com.aoindustries.util.AoCollections;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
	private Timestamp startedTime = null; // Protected by startedLock
	private boolean enabled = false; // Protected by startedLock
	private String display; // Protected by startedLock
	private ExecutorService executorService; // Protected by startLock
	private Set<? extends Node> nodes = Collections.emptySet(); // Protected by startedLock
	private Name localHostname; // Protected by startedLock
	private String localUsername; // Protected by startedLock
	private Node localNode; // Protected by startedLock
	private Set<? extends Resource<?,?>> resources = Collections.emptySet(); // Protected by startedLock

	private final List<ResourceListener> resourceListeners = new ArrayList<>();
	private ExecutorService resourceListenersOnDnsResultExecutorService; // Protected by resourceListeners
	private ExecutorService resourceListenersOnSynchronizationResultExecutorService; // Protected by resourceListeners

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
		this.configuration = new AppClusterPropertiesConfiguration(file);
	}

	/**
	 * Creates a cluster configurated from the provided properties file.
	 * Changes to the properties file will not result in a cluster configuration.
	 * The cluster is not started until <code>start</code> is called.
	 *
	 * @see #start()
	 */
	public AppCluster(Properties properties) {
		this.configuration = new AppClusterPropertiesConfiguration(properties);
	}

	/**
	 * Performs a consistency check on a configuration.
	 */
	/*
	public static void checkConfiguration(AppClusterConfiguration configuration) throws AppClusterConfigurationException {
		checkConfiguration(
			configuration.getNodeConfigurations(),
			configuration.getResourceConfigurations()
		);
	}*/

	/**
	 * Performs a consistency check on a configuration.
	 */
	public static void checkConfiguration(Set<? extends NodeConfiguration> nodeConfigurations, Set<? extends ResourceConfiguration<?,?>> resourceConfigurations) throws AppClusterConfigurationException {
		// Each node must have a distinct display
		Set<String> strings = new HashSet<>(nodeConfigurations.size()*4/3+1);
		for(NodeConfiguration nodeConfiguration : nodeConfigurations) {
			String display = nodeConfiguration.getDisplay();
			if(!strings.add(display)) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.duplicateNodeDisplay", display));
		}

		// Each node must have a distinct hostname
		Set<Name> names = new HashSet<>(nodeConfigurations.size()*4/3+1);
		for(NodeConfiguration nodeConfiguration : nodeConfigurations) {
			Name hostname = nodeConfiguration.getHostname();
			if(!names.add(hostname)) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.duplicateNodeHostname", hostname));
		}

		// Each node must have a distinct display
		strings.clear();
		for(ResourceConfiguration<?,?> resourceConfiguration : resourceConfigurations) {
			String display = resourceConfiguration.getDisplay();
			if(!strings.add(display)) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.duplicateResourceDisplay", display));
		}

		// Each resource-node must have no overlap between nodeRecords and masterRecords of the resource
		for(ResourceConfiguration<?,?> resourceConfiguration : resourceConfigurations) {
			Set<? extends Name> masterRecords = resourceConfiguration.getMasterRecords();
			for(ResourceNodeConfiguration<?,?> resourceNodeConfigs : resourceConfiguration.getResourceNodeConfigurations()) {
				for(Name nodeRecord : resourceNodeConfigs.getNodeRecords()) {
					if(masterRecords.contains(nodeRecord)) {
						throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.nodeMatchesMaster", nodeRecord));
					}
				}
			}
		}

		// Each resource-node must have no overlap between nodeRecords and nodeRecords of any other resource-node of the resource
		for(ResourceConfiguration<?,?> resourceConfiguration : resourceConfigurations) {
			Set<? extends ResourceNodeConfiguration<?,?>> resourceNodeConfigurations = resourceConfiguration.getResourceNodeConfigurations();
			for(ResourceNodeConfiguration<?,?> resourceNodeConfig1 : resourceNodeConfigurations) {
				Set<? extends Name> nodeRecords1 = resourceNodeConfig1.getNodeRecords();
				for(ResourceNodeConfiguration<?,?> resourceNodeConfig2 : resourceNodeConfigurations) {
					if(!resourceNodeConfig1.equals(resourceNodeConfig2)) {
						for(Name nodeRecord : resourceNodeConfig2.getNodeRecords()) {
							if(nodeRecords1.contains(nodeRecord)) {
								throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.checkConfiguration.nodeMatchesOtherNode", nodeRecord));
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Will be called when the resource result has changed in any way.
	 */
	public void addResourceListener(ResourceListener resourceListener) {
		synchronized(resourceListeners) {
			for(ResourceListener existing : resourceListeners) {
				if(existing==resourceListener) return;
			}
			resourceListeners.add(resourceListener);
		}
	}

	/**
	 * Removes listener of resource result changes.
	 */
	public void removeResourceListener(ResourceListener resourceListener) {
		synchronized(resourceListeners) {
			for(int i=0; i<resourceListeners.size(); i++) {
				if(resourceListeners.get(i)==resourceListener) {
					resourceListeners.remove(i);
					return;
				}
			}
		}
	}

	void notifyResourceListenersOnDnsResult(final ResourceDnsResult oldResult, final ResourceDnsResult newResult) {
		synchronized(resourceListeners) {
			for(final ResourceListener resourceListener : resourceListeners) {
				resourceListenersOnDnsResultExecutorService.submit(
					new Runnable() {
						@Override
						public void run() {
							try {
								resourceListener.onResourceDnsResult(oldResult, newResult);
							} catch(Exception exc) {
								logger.log(Level.SEVERE, null, exc);
							}
						}
					}
				);
			}
		}
	}

	void notifyResourceListenersOnSynchronizationResult(final ResourceSynchronizationResult oldResult, final ResourceSynchronizationResult newResult) {
		synchronized(resourceListeners) {
			for(final ResourceListener resourceListener : resourceListeners) {
				resourceListenersOnSynchronizationResultExecutorService.submit(
					new Runnable() {
						@Override
						public void run() {
							try {
								resourceListener.onResourceSynchronizationResult(oldResult, newResult);
							} catch(Exception exc) {
								logger.log(Level.SEVERE, null, exc);
							}
						}
					}
				);
			}
		}
	}

	/**
	 * When the configuration changes, do shutdown and startUp.
	 */
	private final AppClusterConfigurationListener configUpdated = new AppClusterConfigurationListener() {
		@Override
		public void onConfigurationChanged() {
			synchronized(startedLock) {
				if(started) {
					try {
						if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("AppCluster.onConfigurationChanged.info", configuration.getDisplay()));
					} catch(AppClusterConfigurationException exc) {
						logger.log(Level.SEVERE, null, exc);
					}
					shutdown();
					try {
						startUp();
					} catch(AppClusterConfigurationException exc) {
						logger.log(Level.SEVERE, null, exc);
					}
				}
			}
		}
	};

	/**
	 * Checks if this cluster is running.
	 *
	 * @see #start()
	 * @see #stop()
	 */
	public boolean isRunning() {
		synchronized(startedLock) {
			return started;
		}
	}

	/**
	 * Gets the time this cluster was started or <code>null</code> if not running.
	 */
	public Timestamp getStartedTime() {
		synchronized(startedLock) {
			return startedTime;
		}
	}

	/**
	 * Starts this cluster manager.
	 *
	 * @see #stop()
	 */
	public void start() throws AppClusterConfigurationException {
		synchronized(startedLock) {
			if(!started) {
				configuration.start();
				if(logger.isLoggable(Level.INFO)) logger.info(ApplicationResources.accessor.getMessage("AppCluster.start.info", configuration.getDisplay()));
				configuration.addConfigurationListener(configUpdated);
				started = true;
				startedTime = new Timestamp(System.currentTimeMillis());
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
				} catch(AppClusterConfigurationException exc) {
					logger.log(Level.SEVERE, null, exc);
				}
				shutdown();
				started = false;
				startedTime = null;
				configuration.removeConfigurationListener(configUpdated);
				configuration.stop();
			}
		}
	}

	/**
	 * If the cluster is disabled, every node and resource will also be disabled.
	 * A stopped cluster is considered disabled.
	 */
	public boolean isEnabled() {
		synchronized(startedLock) {
			return enabled;
		}
	}

	/**
	 * Gets the display name for this cluster or <code>null</code> if not started.
	 */
	public String getDisplay() {
		synchronized(startedLock) {
			return display;
		}
	}

	@Override
	public String toString() {
		String str = getDisplay();
		return str==null ? super.toString() : str;
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
	 * Gets the set of all nodes or empty set if not started.
	 */
	public Set<? extends Node> getNodes() {
		synchronized(startedLock) {
			return nodes;
		}
	}

	/**
	 * Gets a node given its ID or <code>null</code> if not found.
	 */
	public Node getNode(String id) {
		synchronized(startedLock) {
			for(Node node : nodes) if(node.getId().equals(id)) return node;
			return null;
		}
	}

	/**
	 * Gets a map view of the nodes, keyed by their id.  This is not a fast
	 * implementation and is here for JSP EL compatibility.
	 */
	public Map<String,Node> getNodeMap() {
		Map<String,Node> nodeMap;
		synchronized(startedLock) {
			nodeMap = new LinkedHashMap<>(nodes.size()*4/3+1);
			for(Node node : nodes) nodeMap.put(node.getId(), node);
		}
		return AoCollections.optimalUnmodifiableMap(nodeMap);
	}

	/**
	 * Gets the hostname used to determine which node this server represents
	 * or <code>null</code> if not started.
	 */
	public Name getLocalHostname() {
		synchronized(startedLock) {
			return localHostname;
		}
	}

	/**
	 * Gets the username used to determine which node this server represents
	 * or <code>null</code> if not started.
	 */
	public String getLocalUsername() {
		synchronized(startedLock) {
			return localUsername;
		}
	}

	/**
	 * Gets the node this machine represents or <code>null</code> if this
	 * machine is not one of the nodes.  For this JVM to be considered the local
	 * node, the system hostname must match this node's hostname, and the system
	 * property "user.name" must match this node's username.
	 *
	 * Determined at cluster start time, before any resources are started.
	 *
	 * Returns <code>null</code> when not started.
	 */
	public Node getLocalNode() {
		synchronized(startedLock) {
			return localNode;
		}
	}

	/**
	 * Gets the set of all resources or empty set if not started.
	 */
	public Set<? extends Resource<?,?>> getResources() {
		synchronized(startedLock) {
			return resources;
		}
	}

	/**
	 * Gets a map view of the resources keyed on String resourceId.
	 * This is for compatibility with JSP EL - it is not a fast implementation.
	 */
	public Map<String,? extends Resource<?,?>> getResourceMap() {
		synchronized(startedLock) {
			LinkedHashMap<String,Resource<?,?>> map = new LinkedHashMap<>(resources.size()*4/3+1);
			for(Resource<?,?> resource : resources) map.put(resource.getId(), resource);
			return AoCollections.optimalUnmodifiableMap(map);
		}
	}

	private void startUp() throws AppClusterConfigurationException {
		synchronized(startedLock) {
			assert started;
			try {
				// Get system-local values
				localHostname = Name.fromString(InetAddress.getLocalHost().getCanonicalHostName());
				localUsername = System.getProperty("user.name");

				// Get the configuration values.
				enabled = configuration.isEnabled();
				display = configuration.getDisplay();
				Set<? extends NodeConfiguration> nodeConfigurations = configuration.getNodeConfigurations();
				Set<? extends ResourceConfiguration<?,?>> resourceConfigurations = configuration.getResourceConfigurations();

				// Check the configuration for consistency
				checkConfiguration(nodeConfigurations, resourceConfigurations);

				// Create the nodes
				Set<Node> newNodes = new LinkedHashSet<>(nodeConfigurations.size()*4/3+1);
				for(NodeConfiguration nodeConfiguration : nodeConfigurations) {
					newNodes.add(new Node(this, nodeConfiguration));
				}
				nodes = AoCollections.optimalUnmodifiableSet(newNodes);

				// Find the localNode
				localNode = null;
				for(Node node : nodes) {
					if(
						node.getHostname().equals(localHostname)
						&& node.getUsername().equals(localUsername)
					) {
						localNode = node;
						break;
					}
				}

				// Start the executor services
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
				synchronized(resourceListeners) {
					resourceListenersOnDnsResultExecutorService = Executors.newSingleThreadExecutor(
						new ThreadFactory() {
							@Override
							public Thread newThread(Runnable r) {
								Thread thread = new Thread(r, AppCluster.class.getName()+".resourceListenersOnDnsResultExecutorService");
								thread.setPriority(EXECUTOR_THREAD_PRIORITY);
								return thread;
							}
						}
					);
					resourceListenersOnSynchronizationResultExecutorService = Executors.newSingleThreadExecutor(
						new ThreadFactory() {
							@Override
							public Thread newThread(Runnable r) {
								Thread thread = new Thread(r, AppCluster.class.getName()+".resourceListenersOnSynchronizationResultExecutorService");
								thread.setPriority(EXECUTOR_THREAD_PRIORITY);
								return thread;
							}
						}
					);
				}

				// Start per-resource monitoring and synchronization threads
				Set<Resource<?,?>> newResources = new LinkedHashSet<>(resourceConfigurations.size()*4/3+1);
				for(ResourceConfiguration<?,?> resourceConfiguration : resourceConfigurations) {
					Set<? extends ResourceNodeConfiguration<?,?>> resourceNodeConfigs = resourceConfiguration.getResourceNodeConfigurations();
					Collection<ResourceNode<?,?>> newResourceNodes = new ArrayList<>(resourceNodeConfigs.size());
					for(ResourceNodeConfiguration<?,?> resourceNodeConfig : resourceNodeConfigs) {
						String nodeId = resourceNodeConfig.getNodeId();
						Node node = getNode(nodeId);
						if(node==null) throw new AppClusterConfigurationException(ApplicationResources.accessor.getMessage("AppCluster.startUp.nodeNotFound", resourceConfiguration.getId(), nodeId));
						newResourceNodes.add(resourceNodeConfig.newResourceNode(node));
					}
					Resource<?,?> resource = resourceConfiguration.newResource(this, newResourceNodes);
					newResources.add(resource);
					resource.start();
				}
				resources = AoCollections.optimalUnmodifiableSet(newResources);
			} catch(TextParseException | UnknownHostException exc) {
				throw new AppClusterConfigurationException(exc);
			}
		}
	}

	private void shutdown() {
		synchronized(startedLock) {
			if(started) {
				// Stop per-resource monitoring and synchronization threads
				for(Resource<?,?> resource : resources) resource.stop();
				resources = Collections.emptySet();

				// Stop the executor service
				if(executorService!=null) {
					executorService.shutdown();
					executorService = null;
				}
				synchronized(resourceListeners) {
					if(resourceListenersOnDnsResultExecutorService!=null) {
						resourceListenersOnDnsResultExecutorService.shutdown();
						resourceListenersOnDnsResultExecutorService = null;
					}
					if(resourceListenersOnSynchronizationResultExecutorService!=null) {
						resourceListenersOnSynchronizationResultExecutorService.shutdown();
						resourceListenersOnSynchronizationResultExecutorService = null;
					}
				}

				// Clear the nodes
				nodes = Collections.emptySet();
				localNode = null;
				localHostname = null;
				localUsername = null;

				// Clear the configuration values.
				enabled = false;
				display = null;
			}
		}
	}

	static <T extends Enum<T>> T max(T enum1, T enum2) {
		if(enum1.compareTo(enum2)>0) return enum1;
		return enum2;
	}

	/**
	 * Gets all of the possible statuses for this cluster.
	 * This is primarily for JavaBeans property from JSP EL.
	 */
	public EnumSet<ResourceStatus> getStatuses() {
		return EnumSet.allOf(ResourceStatus.class);
	}

	/**
	 * Gets the overall status of the cluster based on started, enabled, and all resources.
	 */
	public ResourceStatus getStatus() {
		synchronized(startedLock) {
			ResourceStatus status = ResourceStatus.UNKNOWN;
			if(!started) status = max(status, ResourceStatus.STOPPED);
			if(!enabled) status = max(status, ResourceStatus.DISABLED);
			for(Resource<?,?> resource : getResources()) status = max(status, resource.getStatus());
			return status;
		}
	}
}
