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

import java.util.Set;

/**
 * The configuration for one AppCluster manager.
 *
 * @author  AO Industries, Inc.
 */
public interface AppClusterConfiguration {

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
	void addConfigurationListener(AppClusterConfigurationListener listener);

	/**
	 * Removes listener of configuration changes.
	 */
	void removeConfigurationListener(AppClusterConfigurationListener listener);

	/**
	 * @see  AppCluster#isEnabled()
	 */
	boolean isEnabled() throws AppClusterConfigurationException;

	/**
	 * @see  AppCluster#getDisplay()
	 */
	String getDisplay() throws AppClusterConfigurationException;

	/**
	 * Gets the set of nodes for the cluster.
	 */
	Set<? extends NodeConfiguration> getNodeConfigurations() throws AppClusterConfigurationException;

	/**
	 * Gets the set of resources for the cluster.
	 */
	Set<? extends ResourceConfiguration<?,?>> getResourceConfigurations() throws AppClusterConfigurationException;
}
