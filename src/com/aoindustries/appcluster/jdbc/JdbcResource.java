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
package com.aoindustries.appcluster.jdbc;

import com.aoindustries.appcluster.AppCluster;
import com.aoindustries.appcluster.AppClusterConfigurationException;
import com.aoindustries.appcluster.Resource;
import com.aoindustries.appcluster.ResourceConfiguration;
import com.aoindustries.appcluster.ResourceNode;
import java.util.Collection;

/**
 * Resources are synchronized through JDBC.
 *
 * @author  AO Industries, Inc.
 */
public class JdbcResource extends Resource<JdbcResource,JdbcResourceNode> {

    protected JdbcResource(AppCluster cluster, JdbcResourceConfiguration resourceConfiguration, Collection<? extends ResourceNode<?,?>> resourceNodes) throws AppClusterConfigurationException {
        super(cluster, resourceConfiguration, resourceNodes);
    }

    /**
     * Multi master synchronization is not supported for JDBC.
     */
    @Override
    public boolean getAllowMultiMaster() {
        return false;
    }

    @Override
    protected JdbcResourceSynchronizer newResourceSynchronizer(JdbcResourceNode localResourceNode, JdbcResourceNode remoteResourceNode, ResourceConfiguration<JdbcResource,JdbcResourceNode> resourceConfiguration) throws AppClusterConfigurationException {
        JdbcResourceConfiguration jdbcResourceConfiguration = (JdbcResourceConfiguration)resourceConfiguration;
        return new JdbcResourceSynchronizer(
            localResourceNode,
            remoteResourceNode,
            jdbcResourceConfiguration.getSynchronizeSchedule(localResourceNode, remoteResourceNode),
            jdbcResourceConfiguration.getTestSchedule(localResourceNode, remoteResourceNode)
        );
    }
}
