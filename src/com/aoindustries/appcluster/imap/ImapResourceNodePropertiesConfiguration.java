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
package com.aoindustries.appcluster.imap;

import com.aoindustries.appcluster.AppClusterConfigurationException;
import com.aoindustries.appcluster.AppClusterPropertiesConfiguration;
import com.aoindustries.appcluster.CronResourceNodePropertiesConfiguration;
import com.aoindustries.appcluster.Node;

/**
 * The configuration for an IMAP inbox.
 *
 * @author  AO Industries, Inc.
 */
public class ImapResourceNodePropertiesConfiguration extends CronResourceNodePropertiesConfiguration<ImapResource,ImapResourceNode> implements ImapResourceNodeConfiguration {

	protected ImapResourceNodePropertiesConfiguration(AppClusterPropertiesConfiguration properties, String resourceId, String nodeId, String type) throws AppClusterConfigurationException {
		super(properties, resourceId, nodeId);
	}

	@Override
	public ImapResourceNode newResourceNode(Node node) throws AppClusterConfigurationException {
		return new ImapResourceNode(node, this);
	}
}
