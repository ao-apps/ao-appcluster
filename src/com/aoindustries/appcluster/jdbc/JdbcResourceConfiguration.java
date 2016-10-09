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
package com.aoindustries.appcluster.jdbc;

import com.aoindustries.appcluster.AppCluster;
import com.aoindustries.appcluster.AppClusterConfigurationException;
import com.aoindustries.appcluster.CronResourceConfiguration;
import com.aoindustries.appcluster.ResourceNode;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The configuration for a JDBC resource.
 *
 * @author  AO Industries, Inc.
 */
public interface JdbcResourceConfiguration extends CronResourceConfiguration<JdbcResource,JdbcResourceNode> {

	/**
	 * @see JdbcResource#getSchemas()
	 */
	Collection<String> getSchemas();

	/**
	 * @see JdbcResource#getTableTypes()
	 */
	Collection<String> getTableTypes();

	/**
	 * @see JdbcResource#getExcludeTables()
	 */
	Collection<String> getExcludeTables();

	/**
	 * @see JdbcResource#getNoWarnTables()
	 */
	Collection<String> getNoWarnTables();

	/**
	 * @see JdbcResource#getPrepareSlaves()
	 */
	Map<String,String> getPrepareSlaves();

	@Override
	Set<? extends JdbcResourceNodeConfiguration> getResourceNodeConfigurations() throws AppClusterConfigurationException;

	@Override
	JdbcResource newResource(AppCluster cluster, Collection<? extends ResourceNode<?,?>> resourceNodes) throws AppClusterConfigurationException;
}
