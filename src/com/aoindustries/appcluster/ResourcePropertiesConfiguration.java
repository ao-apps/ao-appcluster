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

import java.util.Collection;
import java.util.Set;
import org.xbill.DNS.Name;

/**
 * The configuration for one resource.
 *
 * @author  AO Industries, Inc.
 */
public abstract class ResourcePropertiesConfiguration<R extends Resource<R,RN>,RN extends ResourceNode<R,RN>> implements ResourceConfiguration<R,RN> {

	protected final AppClusterPropertiesConfiguration properties;
	protected final String id;
	protected final boolean enabled;
	protected final String display;
	protected final Set<? extends Name> masterRecords;
	protected final int masterRecordsTtl;
	protected final String type;

	protected ResourcePropertiesConfiguration(AppClusterPropertiesConfiguration properties, String id) throws AppClusterConfigurationException {
		this.properties = properties;
		this.id = id;
		this.enabled = properties.getBoolean("appcluster.resource."+id+".enabled");
		this.display = properties.getString("appcluster.resource."+id+".display", true);
		this.masterRecords = properties.getUniqueNames("appcluster.resource."+id+".masterRecords");
		this.masterRecordsTtl = properties.getInt("appcluster.resource."+id+".masterRecordsTtl");
		this.type = properties.getString("appcluster.resource."+id+".type", true);
	}

	@Override
	public String toString() {
		return display;
	}

	@Override
	public boolean equals(Object o) {
		if(!(o instanceof ResourceConfiguration)) return false;
		return id.equals(((ResourceConfiguration)o).getId());
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public boolean isEnabled() {
		return enabled;
	}

	@Override
	public String getDisplay() {
		return display;
	}

	@Override
	public Set<? extends Name> getMasterRecords() {
		return masterRecords;
	}

	@Override
	public int getMasterRecordsTtl() {
		return masterRecordsTtl;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	abstract public Set<? extends ResourceNodePropertiesConfiguration<R,RN>> getResourceNodeConfigurations() throws AppClusterConfigurationException;

	@Override
	abstract public R newResource(AppCluster cluster, Collection<? extends ResourceNode<?,?>> resourceNodes) throws AppClusterConfigurationException;
}
