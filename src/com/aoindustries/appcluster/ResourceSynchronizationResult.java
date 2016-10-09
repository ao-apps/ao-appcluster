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

import com.aoindustries.util.AoCollections;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;

/**
 * Contains the results of one resource synchronization.
 *
 * @author  AO Industries, Inc.
 */
public class ResourceSynchronizationResult implements ResourceResult {

	private final ResourceNode<?,?> localResourceNode;
	private final ResourceNode<?,?> remoteResourceNode;
	private final ResourceSynchronizationMode mode;
	private final List<ResourceSynchronizationResultStep> steps;

	/**
	 * @param steps At least one step is required.
	 */
	public ResourceSynchronizationResult(
		ResourceNode<?,?> localResourceNode,
		ResourceNode<?,?> remoteResourceNode,
		ResourceSynchronizationMode mode,
		Collection<ResourceSynchronizationResultStep> steps
	) {
		this.localResourceNode = localResourceNode;
		this.remoteResourceNode = remoteResourceNode;
		this.mode = mode;
		if(steps==null) throw new IllegalArgumentException("steps==null");
		if(steps.isEmpty()) throw new IllegalArgumentException("steps.isEmpty()");
		this.steps = AoCollections.unmodifiableCopyList(steps);
	}

	public ResourceNode<?,?> getLocalResourceNode() {
		return localResourceNode;
	}

	public ResourceNode<?,?> getRemoteResourceNode() {
		return remoteResourceNode;
	}

	/**
	 * The start time is the earliest start time of any step.
	 */
	@Override
	public Timestamp getStartTime() {
		long startTime = Long.MAX_VALUE;
		for(ResourceSynchronizationResultStep step : steps) startTime = Math.min(startTime, step.startTime);
		return new Timestamp(startTime);
	}

	/**
	 * The end time is the latest end time of any step.
	 */
	@Override
	public Timestamp getEndTime() {
		long endTime = Long.MIN_VALUE;
		for(ResourceSynchronizationResultStep step : steps) endTime = Math.max(endTime, step.endTime);
		return new Timestamp(endTime);
	}

	/**
	 * The resource status is the highest level resource status of any step.
	 */
	@Override
	public ResourceStatus getResourceStatus() {
		ResourceStatus status = ResourceStatus.UNKNOWN;
		for(ResourceSynchronizationResultStep step : steps) status = AppCluster.max(status, step.getResourceStatus());
		return status;
	}

	/**
	 * Gets the mode of this synchronization.
	 */
	public ResourceSynchronizationMode getMode() {
		return mode;
	}

	/**
	 * Gets the steps for this synchronization.
	 */
	public List<ResourceSynchronizationResultStep> getSteps() {
		return steps;
	}
}
