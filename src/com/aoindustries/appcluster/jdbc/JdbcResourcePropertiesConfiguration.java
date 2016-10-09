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
package com.aoindustries.appcluster.jdbc;

import com.aoindustries.appcluster.AppCluster;
import com.aoindustries.appcluster.AppClusterConfigurationException;
import com.aoindustries.appcluster.AppClusterPropertiesConfiguration;
import com.aoindustries.appcluster.CronResourcePropertiesConfiguration;
import com.aoindustries.appcluster.ResourceNode;
import com.aoindustries.util.AoCollections;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * The configuration for a JDBC resource.
 *
 * @author  AO Industries, Inc.
 */
public class JdbcResourcePropertiesConfiguration extends CronResourcePropertiesConfiguration<JdbcResource,JdbcResourceNode> implements JdbcResourceConfiguration {

	private final Set<String> schemas;
	private final Set<String> tableTypes;
	private final Set<String> excludeTables;
	private final Set<String> noWarnTables;
	private final Map<String,String> prepareSlaves;

	protected JdbcResourcePropertiesConfiguration(AppClusterPropertiesConfiguration properties, String id) throws AppClusterConfigurationException {
		super(properties, id);
		this.schemas = properties.getUniqueStrings("appcluster.resource."+id+"."+type+".schemas", true);
		this.tableTypes = properties.getUniqueStrings("appcluster.resource."+id+"."+type+".tableTypes", true);
		this.excludeTables = properties.getUniqueStrings("appcluster.resource."+id+"."+type+".excludeTables", false);
		this.noWarnTables = properties.getUniqueStrings("appcluster.resource."+id+"."+type+".noWarnTables", false);
		Set<String> prepareSlaveNames = properties.getUniqueStrings("appcluster.resource."+id+"."+type+".prepareSlaves", false);
		if(prepareSlaveNames.isEmpty()) {
			this.prepareSlaves = Collections.emptyMap();
		} else {
			Map<String,String> newPrepareSlaves = new LinkedHashMap<>(prepareSlaveNames.size()*4/3+1);
			for(String prepareSlaveName : prepareSlaveNames) {
				newPrepareSlaves.put(
					prepareSlaveName,
					properties.getString("appcluster.resource."+id+"."+type+".prepareSlave."+prepareSlaveName, true)
				);
			}
			this.prepareSlaves = AoCollections.optimalUnmodifiableMap(newPrepareSlaves);
		}
	}

	@Override
	public Set<String> getSchemas() {
		return schemas;
	}

	@Override
	public Set<String> getTableTypes() {
		return tableTypes;
	}

	@Override
	public Set<String> getExcludeTables() {
		return excludeTables;
	}

	@Override
	public Set<String> getNoWarnTables() {
		return noWarnTables;
	}

	@Override
	public Map<String,String> getPrepareSlaves() {
		return prepareSlaves;
	}

	@Override
	public Set<? extends JdbcResourceNodePropertiesConfiguration> getResourceNodeConfigurations() throws AppClusterConfigurationException {
		String resourceId = getId();
		Set<String> nodeIds = properties.getUniqueStrings("appcluster.resource."+id+".nodes", true);
		Set<JdbcResourceNodePropertiesConfiguration> resourceNodes = new LinkedHashSet<>(nodeIds.size()*4/3+1);
		for(String nodeId : nodeIds) {
			if(!resourceNodes.add(new JdbcResourceNodePropertiesConfiguration(properties, resourceId, nodeId, type))) throw new AssertionError();
		}
		return AoCollections.optimalUnmodifiableSet(resourceNodes);
	}

	@Override
	public JdbcResource newResource(AppCluster cluster, Collection<? extends ResourceNode<?,?>> resourceNodes) throws AppClusterConfigurationException {
		return new JdbcResource(cluster, this, resourceNodes);
	}
}
