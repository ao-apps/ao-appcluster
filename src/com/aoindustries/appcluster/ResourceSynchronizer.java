/*
 * ao-appcluster - Application-level clustering tools.
 * Copyright (C) 2011, 2016  AO Industries, Inc.
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

/**
 * <p>
 * Each resource has one synchronizer between it and any other node for the
 * resource.
 * </p>
 * <p>
 * Every resource type should support testing.  The test should ensure point-in-time
 * consistency between two nodes as much as possible.
 * </p>
 * <p>
 * Resources are only required to support synchronization when is a master.
 * They may optionally support synchronization when a slave (such as a csync2
 * master on a slave node).
 * </p>
 *
 * @author  AO Industries, Inc.
 */
abstract public class ResourceSynchronizer<R extends Resource<R,RN>,RN extends ResourceNode<R,RN>> {

	protected final RN localResourceNode;
	protected final RN remoteResourceNode;

	protected ResourceSynchronizer(RN localResourceNode, RN remoteResourceNode) {
		R resource = localResourceNode.getResource();
		if(resource != remoteResourceNode.getResource()) throw new IllegalArgumentException("localResourceNode.resource != remoteResourceNode.resource");
		if(!localResourceNode.getNode().equals(resource.getCluster().getLocalNode())) throw new IllegalArgumentException("localResourceNode.node != localResourceNode.resource.cluster.localNode");
		this.localResourceNode = localResourceNode;
		this.remoteResourceNode = remoteResourceNode;
	}

	@Override
	public String toString() {
		return localResourceNode.getResource()+": "+localResourceNode.getNode()+" -> "+remoteResourceNode.getNode();
	}

	/**
	 * Gets the local resource node.
	 */
	public RN getLocalResourceNode() {
		return localResourceNode;
	}

	/**
	 * Gets the remote resource node.
	 */
	public RN getRemoteResourceNode() {
		return remoteResourceNode;
	}

	/**
	 * Gets the current synchronization state.
	 */
	abstract public ResourceSynchronizerState getState();

	/**
	 * Gets a description of the current state or <code>null</code> for no
	 * specific message.
	 */
	abstract public String getStateMessage();

	/**
	 * Schedules an immediate synchronization if possible.
	 */
	abstract public void synchronizeNow(ResourceSynchronizationMode mode);

	/**
	 * Gets the last synchronization result or <code>null</code> if unavailable.
	 */
	abstract public ResourceSynchronizationResult getLastResult();

	/**
	 * Gets the synchronization result status.  Considered as STOPPED/DISABLED/STARTING if last result is not available.
	 */
	public ResourceStatus getResultStatus() {
		ResourceSynchronizationResult result = getLastResult();
		if(result!=null) return result.getResourceStatus();
		// No result, base off synchronizer state for STOPPED/DISABLED/STARTING
		ResourceStatus status = getState().getResourceStatus();
		return status==ResourceStatus.HEALTHY ? ResourceStatus.STARTING : status;
	}

	/**
	 * Starts the synchronizer.
	 */
	abstract protected void start();

	/**
	 * Stops the synchronizer.
	 */
	abstract protected void stop();
}
