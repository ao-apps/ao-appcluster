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
import org.xbill.DNS.Name;

/**
 * The configuration for a node.
 *
 * @author  AO Industries, Inc.
 */
public class NodePropertiesConfiguration implements NodeConfiguration {

	protected final AppClusterPropertiesConfiguration properties;
	protected final String id;
	protected final boolean enabled;
	protected final String display;
	protected final Name hostname;
	protected final String username;
	protected final Set<? extends Name> nameservers;

	protected NodePropertiesConfiguration(AppClusterPropertiesConfiguration properties, String id) throws AppClusterConfigurationException {
		this.properties = properties;
		this.id = id;
		this.enabled = properties.getBoolean("appcluster.node."+id+".enabled");
		this.display = properties.getString("appcluster.node."+id+".display", true);
		this.hostname = properties.getName("appcluster.node."+id+".hostname");
		this.username = properties.getString("appcluster.node."+id+".username", true);
		this.nameservers = properties.getUniqueNames("appcluster.node."+id+".nameservers");
	}

	@Override
	public String toString() {
		return display;
	}

	@Override
	public boolean equals(Object o) {
		if(!(o instanceof NodeConfiguration)) return false;
		return id.equals(((NodeConfiguration)o).getId());
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
	public Name getHostname() {
		return hostname;
	}

	@Override
	public String getUsername() {
		return username;
	}

	@Override
	public Set<? extends Name> getNameservers() {
		return nameservers;
	}
}
