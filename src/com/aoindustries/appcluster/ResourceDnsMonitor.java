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

import com.aoindustries.util.ErrorPrinter;
import com.aoindustries.util.StringUtility;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Name;
import org.xbill.DNS.Record;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Type;

/**
 * Monitors the status of a resource by monitoring its role based on DNS entries
 * Monitors DNS entries to determine which nodes are masters and which are slaves
 * while being careful to detect any inconsistent states.
 *
 * @author  AO Industries, Inc.
 */
public class ResourceDnsMonitor {

	private static final Logger logger = Logger.getLogger(ResourceDnsMonitor.class.getName());

	private static final int THREAD_PRIORITY = Thread.NORM_PRIORITY - 1;

	/**
	 * Checks the DNS settings once every 30 seconds.
	 */
	public static final int DNS_CHECK_INTERVAL = 30000;

	/**
	 * The number of DNS tries.
	 */
	public static final int DNS_ATTEMPTS = 2;

	/**
	 * DNS queries time-out at 30 seconds.
	 */
	public static final int DNS_CHECK_TIMEOUT = 30000;

	/**
	 * Only one resolver will be created for each unique nameserver (case-insensitive on unique)
	 */
	private static final ConcurrentMap<Nameserver,SimpleResolver> resolvers = new ConcurrentHashMap<>();
	private static SimpleResolver getSimpleResolver(Nameserver hostname) throws UnknownHostException {
		SimpleResolver resolver = resolvers.get(hostname);
		if(resolver==null) {
			resolver = new SimpleResolver(hostname.toString());
			resolver.setTimeout(DNS_CHECK_TIMEOUT / 1000, DNS_CHECK_TIMEOUT % 1000);
			SimpleResolver existing = resolvers.putIfAbsent(hostname, resolver);
			if(existing!=null) resolver = existing;
		}
		return resolver;
	}

	/**
	 * Gets a mapping for all nodes with the same status.
	 */
	private static Map<? extends Node,? extends ResourceNodeDnsResult> getNodeResults(Resource<?,?> resource, Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> nodeRecordLookups, NodeDnsStatus nodeStatus, Collection<String> nodeStatusMessages) {
		Set<? extends ResourceNode<?,?>> resourceNodes = resource.getResourceNodes();
		Map<Node,ResourceNodeDnsResult> nodeResults = new HashMap<>(resourceNodes.size()*4/3+1);
		for(ResourceNode<?,?> resourceNode : resourceNodes) {
			nodeResults.put(
				resourceNode.getNode(),
				new ResourceNodeDnsResult(
					resourceNode,
					nodeRecordLookups,
					nodeStatus,
					nodeStatusMessages
				)
			);
		}
		return nodeResults;
	}

	private final Resource<?,?> resource;

	private final Object threadLock = new Object();
	private Thread thread; // All access uses threadLock
	private ResourceDnsResult lastResult; // All access uses threadLock

	ResourceDnsMonitor(Resource<?,?> resource) {
		this.resource = resource;
		long currentTime = System.currentTimeMillis();
		this.lastResult = new ResourceDnsResult(
			resource,
			currentTime,
			currentTime,
			null,
			MasterDnsStatus.STOPPED,
			null,
			getNodeResults(resource, null, NodeDnsStatus.STOPPED, null)
		);
	}

	/**
	 * Gets the resource this monitor is for.
	 */
	public Resource<?,?> getResource() {
		return resource;
	}

	private void setDnsResult(ResourceDnsResult newResult) {
		assert Thread.holdsLock(threadLock);
		ResourceDnsResult oldResult = this.lastResult;
		this.lastResult = newResult;

		// Notify listeners
		resource.getCluster().notifyResourceListenersOnDnsResult(oldResult, newResult);
	}

	/**
	 * Gets the last result.
	 */
	public ResourceDnsResult getLastResult() {
		synchronized(threadLock) {
			return lastResult;
		}
	}

	private static final Name[] emptySearchPath = new Name[0];

	/**
	 * If both the cluster and this resource are enabled, starts the resource DNS monitor.
	 */
	void start() {
		synchronized(threadLock) {
			if(!resource.getCluster().isEnabled()) {
				long currentTime = System.currentTimeMillis();
				Collection<String> messages = Collections.singleton(ApplicationResources.accessor.getMessage("ResourceDnsMonitor.start.clusterDisabled.statusMessage"));
				setDnsResult(
					new ResourceDnsResult(
						resource,
						currentTime,
						currentTime,
						null,
						MasterDnsStatus.DISABLED,
						messages,
						getNodeResults(resource, null, NodeDnsStatus.DISABLED, messages)
					)
				);
			} else if(!resource.isEnabled()) {
				long currentTime = System.currentTimeMillis();
				Collection<String> messages = Collections.singleton(ApplicationResources.accessor.getMessage("ResourceDnsMonitor.start.resourceDisabled.statusMessage"));
				setDnsResult(
					new ResourceDnsResult(
						resource,
						currentTime,
						currentTime,
						null,
						MasterDnsStatus.DISABLED,
						messages,
						getNodeResults(resource, null, NodeDnsStatus.DISABLED, messages)
					)
				);
			} else {
				if(thread==null) {
					long currentTime = System.currentTimeMillis();
					Collection<String> unknownMessage = Collections.singleton(ApplicationResources.accessor.getMessage("ResourceDnsMonitor.start.newThread.statusMessage"));
					Collection<String> nodeDisabledMessages = Collections.singleton(ApplicationResources.accessor.getMessage("ResourceDnsMonitor.nodeDisabled"));
					Set<? extends ResourceNode<?,?>> resourceNodes = resource.getResourceNodes();
					Map<Node,ResourceNodeDnsResult> nodeResults = new HashMap<>(resourceNodes.size()*4/3+1);
					for(ResourceNode<?,?> resourceNode : resourceNodes) {
						Node node = resourceNode.getNode();
						if(node.isEnabled()) {
							nodeResults.put(
								node,
								new ResourceNodeDnsResult(resourceNode, null, NodeDnsStatus.STARTING, unknownMessage)
							);
						} else {
							nodeResults.put(
								node,
								new ResourceNodeDnsResult(resourceNode, null, NodeDnsStatus.DISABLED, nodeDisabledMessages)
							);
						}
					}
					setDnsResult(
						new ResourceDnsResult(
							resource,
							currentTime,
							currentTime,
							null,
							MasterDnsStatus.STARTING,
							unknownMessage,
							nodeResults
						)
					);
					final ExecutorService executorService = resource.getCluster().getExecutorService();
					thread = new Thread(
						new Runnable() {
							@Override
							public void run() {
								final Thread currentThread = Thread.currentThread();
								final Set<? extends Name> masterRecords = resource.getMasterRecords();
								final int masterRecordsTtl = resource.getMasterRecordsTtl();
								final boolean allowMultiMaster = resource.getAllowMultiMaster();
								final Nameserver[] enabledNameservers = resource.getEnabledNameservers().toArray(new Nameserver[resource.getEnabledNameservers().size()]);

								final ResourceNode<?,?>[] resourceNodes = resource.getResourceNodes().toArray(new ResourceNode<?,?>[resource.getResourceNodes().size()]);

								// Find all the unique hostnames and nameservers that will be queried
								final Name[] allHostnames;
								{
									final Set<Name> allHostnamesSet = new HashSet<>();
									allHostnamesSet.addAll(masterRecords);
									for(ResourceNode<?,?> resourceNode : resourceNodes) {
										if(resourceNode.getNode().isEnabled()) allHostnamesSet.addAll(resourceNode.getNodeRecords());
									}
									allHostnames = allHostnamesSet.toArray(new Name[allHostnamesSet.size()]);
								}

								while(true) {
									synchronized(threadLock) {
										if(currentThread!=thread) break;
									}
									try {
										long startTime = System.currentTimeMillis();

										// Query all enabled nameservers for all involved dns entries in parallel, getting all A records
										final Map<Name,Map<Nameserver,Future<DnsLookupResult>>> allHostnameFutures = new HashMap<>(allHostnames.length*4/3+1);
										for(final Name hostname : allHostnames) {
											Map<Nameserver,Future<DnsLookupResult>> hostnameFutures = new HashMap<>(enabledNameservers.length*4/3+1);
											allHostnameFutures.put(hostname, hostnameFutures);
											for(final Nameserver nameserver : enabledNameservers) {
												hostnameFutures.put(
													nameserver,
													executorService.submit(
														new Callable<DnsLookupResult>() {
															@Override
															public DnsLookupResult call() {
																try {
																	for(int attempt=0; attempt<DNS_ATTEMPTS; attempt++) {
																		Lookup lookup = new Lookup(hostname, Type.A);
																		lookup.setCache(null);
																		lookup.setResolver(getSimpleResolver(nameserver));
																		lookup.setSearchPath(emptySearchPath);
																		Record[] records = lookup.run();
																		int result = lookup.getResult();
																		switch(result) {
																			case Lookup.SUCCESSFUL :
																				if(records==null || records.length==0) {
																					return new DnsLookupResult(
																						hostname,
																						DnsLookupStatus.HOST_NOT_FOUND,
																						null,
																						null
																					);
																				}
																				String[] addresses = new String[records.length];
																				Collection<String> statusMessages = null;
																				for(int c=0;c<records.length;c++) {
																					ARecord aRecord = (ARecord)records[c];
																					// Verify masterDomain TTL settings match expected values, issue as a warning
																					if(masterRecords.contains(hostname)) {
																						long ttl = aRecord.getTTL();
																						if(ttl!=masterRecordsTtl) {
																							if(statusMessages==null) statusMessages = new ArrayList<>();
																							statusMessages.add(ApplicationResources.accessor.getMessage("ResourceDnsMonitor.lookup.unexpectedTtl", masterRecordsTtl, ttl));
																						}
																					}
																					addresses[c] = aRecord.getAddress().getHostAddress();
																				}
																				return new DnsLookupResult(
																					hostname,
																					statusMessages==null ? DnsLookupStatus.SUCCESSFUL : DnsLookupStatus.WARNING,
																					statusMessages,
																					addresses
																				);
																			case Lookup.UNRECOVERABLE :
																				return new DnsLookupResult(
																					hostname,
																					DnsLookupStatus.UNRECOVERABLE,
																					null,
																					null
																				);
																			case Lookup.TRY_AGAIN :
																				// Fall-through to try again loop
																				break;
																			case Lookup.HOST_NOT_FOUND :
																				return new DnsLookupResult(
																					hostname,
																					DnsLookupStatus.HOST_NOT_FOUND,
																					null,
																					null
																				);
																			case Lookup.TYPE_NOT_FOUND :
																				return new DnsLookupResult(
																					hostname,
																					DnsLookupStatus.TYPE_NOT_FOUND,
																					null,
																					null
																				);
																			default :
																				return new DnsLookupResult(
																					hostname,
																					DnsLookupStatus.ERROR,
																					Collections.singleton(ApplicationResources.accessor.getMessage("ResourceDnsMonitor.lookup.unexpectedResultCode", result)),
																					null
																				);
																		}
																	}
																	return new DnsLookupResult(
																		hostname,
																		DnsLookupStatus.TRY_AGAIN,
																		null,
																		null
																	);
																} catch(Exception exc) {
																	return new DnsLookupResult(
																		hostname,
																		DnsLookupStatus.ERROR,
																		Collections.singleton(ErrorPrinter.getStackTraces(exc)),
																		null
																	);
																}
															}
														}
													)
												);
											}
										}

										// Get all the masterRecord results
										Map<Name,Map<Nameserver,DnsLookupResult>> masterRecordLookups = new HashMap<>(masterRecords.size()*4/3+1);
										MasterDnsStatus masterStatus = MasterDnsStatus.CONSISTENT;
										List<String> masterStatusMessages = new ArrayList<>();
										Nameserver firstMasterNameserver = null;
										Name firstMasterRecord = null;
										Set<String> firstMasterAddresses = null;
										for(Name masterRecord : masterRecords) {
											Map<Nameserver,DnsLookupResult> masterLookups = new HashMap<>(enabledNameservers.length*4/3+1);
											masterRecordLookups.put(masterRecord, masterLookups);
											Map<Nameserver,Future<DnsLookupResult>> masterFutures = allHostnameFutures.get(masterRecord);
											boolean foundSuccessful = false;
											for(Nameserver enabledNameserver : enabledNameservers) {
												try {
													DnsLookupResult result = masterFutures.get(enabledNameserver).get();
													masterLookups.put(enabledNameserver, result);
													if(result.getStatus()==DnsLookupStatus.SUCCESSFUL || result.getStatus()==DnsLookupStatus.WARNING) {
														if(result.getStatus()==DnsLookupStatus.WARNING) masterStatus = AppCluster.max(masterStatus, MasterDnsStatus.WARNING);
														foundSuccessful = true;
														Set<String> addresses = result.getAddresses();
														// Check for multi-master violation
														if(addresses.size()>1 && !allowMultiMaster) {
															masterStatus = AppCluster.max(masterStatus, MasterDnsStatus.INCONSISTENT);
															masterStatusMessages.add(
																ApplicationResources.accessor.getMessage(
																	"ResourceDnsMonitor.masterRecord.multiMasterNotAllowed",
																	enabledNameserver,
																	StringUtility.join(addresses, ", ")
																)
															);
														}
														if(firstMasterAddresses==null) {
															firstMasterNameserver = enabledNameserver;
															firstMasterRecord = masterRecord;
															firstMasterAddresses = addresses;
														} else {
															// All multi-record masters must have the same IP address(es) within a single node (like for domain aliases)
															if(!firstMasterAddresses.equals(addresses)) {
																masterStatus = AppCluster.max(masterStatus, MasterDnsStatus.INCONSISTENT);
																masterStatusMessages.add(
																	ApplicationResources.accessor.getMessage(
																		"ResourceDnsMonitor.multiRecordMaster.mismatch",
																		firstMasterNameserver,
																		firstMasterRecord,
																		StringUtility.join(firstMasterAddresses, ", "),
																		enabledNameserver,
																		masterRecord,
																		StringUtility.join(addresses, ", ")
																	)
																);
															}
														}
													}
												} catch(Exception exc) {
													masterLookups.put(
														enabledNameserver,
														new DnsLookupResult(
															masterRecord,
															DnsLookupStatus.UNRECOVERABLE,
															Collections.singleton(ErrorPrinter.getStackTraces(exc)),
															null
														)
													);
												}
											}
											// Make sure we got at least one response for every master
											if(!foundSuccessful) {
												masterStatus = AppCluster.max(masterStatus, MasterDnsStatus.INCONSISTENT);
												masterStatusMessages.add(ApplicationResources.accessor.getMessage("ResourceDnsMonitor.masterRecord.missing", masterRecord));
											}
										}

										// Get the results for each node
										Map<Node,ResourceNodeDnsResult> nodeResults = new HashMap<>(resourceNodes.length*4/3+1);
										Set<String> allNodeAddresses = new HashSet<>(resourceNodes.length*4/3+1);
										for(ResourceNode<?,?> resourceNode :  resourceNodes) {
											Node node = resourceNode.getNode();
											if(node.isEnabled()) {
												Set<? extends Name> nodeRecords = resourceNode.getNodeRecords();
												Map<Name,Map<Nameserver,DnsLookupResult>> nodeRecordLookups = new HashMap<>(nodeRecords.size()*4/3+1);
												NodeDnsStatus nodeStatus = NodeDnsStatus.SLAVE;
												List<String> nodeStatusMessages = new ArrayList<>();
												Nameserver firstNodeNameserver = null;
												Name firstNodeRecord = null;
												Set<String> firstNodeAddresses = null;
												for(Name nodeRecord : resourceNode.getNodeRecords()) {
													Map<Nameserver,DnsLookupResult> nodeLookups = new HashMap<>(enabledNameservers.length*4/3+1);
													nodeRecordLookups.put(nodeRecord, nodeLookups);
													Map<Nameserver,Future<DnsLookupResult>> nodeFutures = allHostnameFutures.get(nodeRecord);
													boolean foundSuccessful = false;
													for(Nameserver enabledNameserver : enabledNameservers) {
														try {
															DnsLookupResult result = nodeFutures.get(enabledNameserver).get();
															nodeLookups.put(enabledNameserver, result);
															if(result.getStatus()==DnsLookupStatus.SUCCESSFUL || result.getStatus()==DnsLookupStatus.WARNING) {
																foundSuccessful = true;
																Set<String> addresses = result.getAddresses();
																allNodeAddresses.addAll(addresses);
																// Must be only one A record
																if(addresses.size()>1) {
																	nodeStatus = NodeDnsStatus.INCONSISTENT;
																	nodeStatusMessages.add(
																		ApplicationResources.accessor.getMessage(
																			"ResourceDnsMonitor.nodeRecord.onlyOneAllowed",
																			StringUtility.join(addresses, ", ")
																		)
																	);
																} else {
																	// Each node must have a different A record
																	String address = addresses.iterator().next();
																	for(ResourceNodeDnsResult previousNodeResult :  nodeResults.values()) {
																		Map<? extends Name,? extends Map<? extends Nameserver,? extends DnsLookupResult>> previousNodeRecordLookups = previousNodeResult.getNodeRecordLookups();
																		if(previousNodeRecordLookups!=null) {
																			boolean foundMatch = false;
																			MATCH_LOOP:
																			for(Map<? extends Nameserver,? extends DnsLookupResult> previousLookups : previousNodeRecordLookups.values()) {
																				for(DnsLookupResult previousResult : previousLookups.values()) {
																					if(previousResult.getAddresses().contains(address)) {
																						foundMatch = true;
																						break MATCH_LOOP;
																					}
																				}
																			}
																			if(foundMatch) {
																				Node previousNode = previousNodeResult.getResourceNode().getNode();
																				nodeStatus = NodeDnsStatus.INCONSISTENT;
																				nodeStatusMessages.add(
																					ApplicationResources.accessor.getMessage(
																						"ResourceDnsMonitor.nodeRecord.duplicateA",
																						previousNode,
																						nodeRecord,
																						address
																					)
																				);
																				// Re-add the previous with inconsistent state and additional message
																				List<String> newNodeStatusMessages = new ArrayList<>(previousNodeResult.getNodeStatusMessages());
																				newNodeStatusMessages.add(
																					ApplicationResources.accessor.getMessage(
																						"ResourceDnsMonitor.nodeRecord.duplicateA",
																						nodeRecord,
																						previousNode,
																						address
																					)
																				);
																				nodeResults.put(
																					previousNode,
																					new ResourceNodeDnsResult(
																						previousNodeResult.getResourceNode(),
																						previousNodeResult.getNodeRecordLookups(),
																						NodeDnsStatus.INCONSISTENT,
																						newNodeStatusMessages
																					)
																				);
																			}
																		}
																	}
																}
																if(firstNodeAddresses==null) {
																	firstNodeNameserver = enabledNameserver;
																	firstNodeRecord = nodeRecord;
																	firstNodeAddresses = addresses;
																} else {
																	// All multi-record nodes must have the same IP address within a single node (like for domain aliases)
																	if(!firstNodeAddresses.equals(addresses)) {
																		nodeStatus = NodeDnsStatus.INCONSISTENT;
																		nodeStatusMessages.add(
																			ApplicationResources.accessor.getMessage(
																				"ResourceDnsMonitor.multiRecordNode.mismatch",
																				firstNodeNameserver,
																				firstNodeRecord,
																				StringUtility.join(firstNodeAddresses, ", "),
																				enabledNameserver,
																				nodeRecord,
																				StringUtility.join(addresses, ", ")
																			)
																		);
																	}
																}
															}
														} catch(Exception exc) {
															nodeLookups.put(
																enabledNameserver,
																new DnsLookupResult(
																	nodeRecord,
																	DnsLookupStatus.UNRECOVERABLE,
																	Collections.singleton(ErrorPrinter.getStackTraces(exc)),
																	null
																)
															);
														}
													}
													// Make sure we got at least one response for every node
													if(!foundSuccessful) {
														nodeStatus = NodeDnsStatus.INCONSISTENT;
														nodeStatusMessages.add(ApplicationResources.accessor.getMessage("ResourceDnsMonitor.nodeRecord.missing", nodeRecord));
													}
												}
												// If master and node are both consistent and matches any master A record, promote to master
												if((masterStatus==MasterDnsStatus.CONSISTENT || masterStatus==MasterDnsStatus.WARNING) && nodeStatus==NodeDnsStatus.SLAVE) {
													if(firstMasterAddresses.containsAll(firstNodeAddresses)) nodeStatus=NodeDnsStatus.MASTER;
												}
												nodeResults.put(
													node,
													new ResourceNodeDnsResult(
														resourceNode,
														nodeRecordLookups,
														nodeStatus,
														nodeStatusMessages
													)
												);
											} else {
												// Node disabled
												nodeResults.put(
													node,
													new ResourceNodeDnsResult(
														resourceNode,
														null,
														NodeDnsStatus.DISABLED,
														Collections.singleton(ApplicationResources.accessor.getMessage("ResourceDnsMonitor.nodeDisabled"))
													)
												);
											}
										}

										// Inconsistent if any master A record is outside the expected nodeDomains
										for(Name masterRecord : masterRecords) {
											for(Map<Nameserver,DnsLookupResult> masterLookups : masterRecordLookups.values()) {
												for(DnsLookupResult masterResult : masterLookups.values()) {
													for(String masterAddress : masterResult.getAddresses()) {
														if(!allNodeAddresses.contains(masterAddress)) {
															masterStatus = AppCluster.max(masterStatus, MasterDnsStatus.INCONSISTENT);
															masterStatusMessages.add(
																ApplicationResources.accessor.getMessage(
																	"ResourceDnsMonitor.masterARecordDoesntMatchNode",
																	masterRecord,
																	masterAddress
																)
															);
														}
													}
												}
											}
										}

										synchronized(threadLock) {
											if(currentThread!=thread) break;
											setDnsResult(
												new ResourceDnsResult(
													resource,
													startTime,
													System.currentTimeMillis(),
													masterRecordLookups,
													masterStatus,
													masterStatusMessages,
													nodeResults
												)
											);
										}
									} catch(RejectedExecutionException exc) {
										// Normal during shutdown
										boolean needsLogged;
										synchronized(threadLock) {
											needsLogged = currentThread==thread;
										}
										if(needsLogged) logger.log(Level.SEVERE, null, exc);
									} catch(Exception exc) {
										logger.log(Level.SEVERE, null, exc);
									}
									try {
										Thread.sleep(DNS_CHECK_INTERVAL);
									} catch(InterruptedException exc) {
										logger.log(Level.WARNING, null, exc);
										// Restore the interrupted status
										Thread.currentThread().interrupt();
									}
								}
							}
						},
						"PropertiesConfiguration.fileMonitorThread"
					);
					thread.setPriority(THREAD_PRIORITY);
					thread.start();
				}
			}
		}
	}

	/**
	 * Stops this resource DNS monitor.
	 */
	void stop() {
		long currentTime = System.currentTimeMillis();
		synchronized(threadLock) {
			thread = null;
			setDnsResult(
				new ResourceDnsResult(
					resource,
					currentTime,
					currentTime,
					null,
					MasterDnsStatus.STOPPED,
					null,
					getNodeResults(resource, null, NodeDnsStatus.STOPPED, null)
				)
			);
		}
	}
}
