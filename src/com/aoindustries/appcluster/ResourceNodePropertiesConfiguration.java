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
 * The configuration for one resource node.
 *
 * @author  AO Industries, Inc.
 */
public abstract class ResourceNodePropertiesConfiguration<R extends Resource<R,RN>,RN extends ResourceNode<R,RN>> implements ResourceNodeConfiguration<R,RN> {

	protected final AppClusterPropertiesConfiguration properties;
	protected final String resourceId;
	protected final String nodeId;
	protected final Set<? extends Name> nodeRecords;

	protected ResourceNodePropertiesConfiguration(AppClusterPropertiesConfiguration properties, String resourceId, String nodeId) throws AppClusterConfigurationException {
		this.properties = properties;
		this.resourceId = resourceId;
		this.nodeId = nodeId;
		this.nodeRecords = properties.getUniqueNames("appcluster.resource."+resourceId+".node."+nodeId+".nodeRecords");
	}

	@Override
	public String toString() {
		return resourceId+'/'+nodeId;
	}

	@Override
	public boolean equals(Object o) {
		if(!(o instanceof ResourceNodeConfiguration)) return false;
		ResourceNodeConfiguration<?,?> other = (ResourceNodeConfiguration)o;
		return
			resourceId.equals(other.getResourceId())
			&& nodeId.equals(other.getNodeId())
		;
	}

	@Override
	public int hashCode() {
		return resourceId.hashCode() * 31 + nodeId.hashCode();
	}

	@Override
	public String getResourceId() {
		return resourceId;
	}

	@Override
	public String getNodeId() {
		return nodeId;
	}

	@Override
	public Set<? extends Name> getNodeRecords() {
		return nodeRecords;
	}

	@Override
	public abstract RN newResourceNode(Node node) throws AppClusterConfigurationException;
}
