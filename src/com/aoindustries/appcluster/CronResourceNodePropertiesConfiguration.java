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
 * The configuration for a cron resource.
 *
 * @author  AO Industries, Inc.
 */
abstract public class CronResourceNodePropertiesConfiguration<R extends CronResource<R,RN>,RN extends CronResourceNode<R,RN>> extends ResourceNodePropertiesConfiguration<R,RN> implements CronResourceNodeConfiguration<R,RN> {

	protected CronResourceNodePropertiesConfiguration(AppClusterPropertiesConfiguration properties, String resourceId, String nodeId) throws AppClusterConfigurationException {
		super(properties, resourceId, nodeId);
	}

	@Override
	abstract public RN newResourceNode(Node node) throws AppClusterConfigurationException;
}
