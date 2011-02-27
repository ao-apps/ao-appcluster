/*
 * ao-appcluster - Coordinates system components installed in master/slave replication.
 * Copyright (C) 2011  AO Industries, Inc.
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

import com.aoindustries.appcluster.AppClusterConfiguration.AppClusterConfigurationException;
import java.util.Collection;

/**
 * Resources are manually synchronized, such as through code being deployed automatically to every
 * node.  This performs DNS monitoring and role determination, but without any monitoring or
 * synchronization of data.
 *
 * @author  AO Industries, Inc.
 */
public class ManualResource extends Resource<ManualResource,ManualResourceNode> {

    static final String TYPE = "manual";

    private final boolean allowMultiMaster;

    ManualResource(AppCluster cluster, AppClusterConfiguration.ManualResourceConfiguration resourceConfiguration, Collection<ManualResourceNode> resourceNodes) throws AppClusterConfigurationException {
        super(cluster, resourceConfiguration, resourceNodes);
        this.allowMultiMaster = resourceConfiguration.getAllowMultiMaster();
    }

    @Override
    public boolean getAllowMultiMaster() {
        return allowMultiMaster;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
