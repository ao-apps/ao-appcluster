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
import com.aoindustries.appcluster.CronResourceConfiguration;
import com.aoindustries.appcluster.ResourceNode;
import java.util.Collection;
import java.util.Set;

/**
 * The configuration for a csync2 resource.
 *
 * @author  AO Industries, Inc.
 */
public interface Csync2ResourceConfiguration extends CronResourceConfiguration<Csync2Resource,Csync2ResourceNode> {

	/**
	 * @see Csync2Resource#getAllowMultiMaster()
	 */
	boolean getAllowMultiMaster();

	/**
	 * Gets all the groups that will be synchronized by csync2 for this resource.
	 */
	Collection<String> getGroups();

	@Override
	Set<? extends Csync2ResourceNodeConfiguration> getResourceNodeConfigurations() throws AppClusterConfigurationException;

	@Override
	Csync2Resource newResource(AppCluster cluster, Collection<? extends ResourceNode<?,?>> resourceNodes) throws AppClusterConfigurationException;
}
