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
package com.aoindustries.appcluster.csync2;

import com.aoindustries.appcluster.AppCluster;
import com.aoindustries.appcluster.AppClusterConfigurationException;
import com.aoindustries.appcluster.CronResource;
import com.aoindustries.appcluster.ResourceConfiguration;
import com.aoindustries.appcluster.ResourceNode;
import com.aoindustries.util.AoCollections;
import java.util.Collection;
import java.util.Set;

/**
 * Synchronizes resources using csync2.
 *
 * @author  AO Industries, Inc.
 */
public class Csync2Resource extends CronResource<Csync2Resource,Csync2ResourceNode> {

	private final boolean allowMultiMaster;
	private final Set<String> groups;

	protected Csync2Resource(AppCluster cluster, Csync2ResourceConfiguration resourceConfiguration, Collection<? extends ResourceNode<?,?>> resourceNodes) throws AppClusterConfigurationException {
		super(cluster, resourceConfiguration, resourceNodes);
		this.allowMultiMaster = resourceConfiguration.getAllowMultiMaster();
		this.groups = AoCollections.unmodifiableCopySet(resourceConfiguration.getGroups());
	}

	@Override
	public boolean getAllowMultiMaster() {
		return allowMultiMaster;
	}

	public Set<String> getGroups() {
		return groups;
	}

	@Override
	protected Csync2ResourceSynchronizer newResourceSynchronizer(Csync2ResourceNode localResourceNode, Csync2ResourceNode remoteResourceNode, ResourceConfiguration<Csync2Resource,Csync2ResourceNode> resourceConfiguration) throws AppClusterConfigurationException {
		Csync2ResourceConfiguration csync2ResourceConfiguration = (Csync2ResourceConfiguration)resourceConfiguration;
		return new Csync2ResourceSynchronizer(
			localResourceNode,
			remoteResourceNode,
			csync2ResourceConfiguration.getSynchronizeSchedule(localResourceNode, remoteResourceNode),
			csync2ResourceConfiguration.getTestSchedule(localResourceNode, remoteResourceNode)
		);
	}
}
