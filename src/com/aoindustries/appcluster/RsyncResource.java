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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Synchronizes resources using rsync.
 *
 * @author  AO Industries, Inc.
 */
public class RsyncResource extends Resource<RsyncResource,RsyncResourceNode> {

    //private static final Logger logger = Logger.getLogger(RsyncResource.class.getName());

    private final boolean delete;

    private final Map<Node,RsyncResourceNode> resourceNodes;

    RsyncResource(AppCluster cluster, AppClusterConfiguration.RsyncResourceConfiguration resourceConfiguration) throws AppClusterConfigurationException {
        super(cluster, resourceConfiguration);
        Set<? extends AppClusterConfiguration.ResourceNodeConfiguration> nodeConfigs = resourceConfiguration.getResourceNodeConfigurations();
        Map<Node,RsyncResourceNode> newResourceNodes = new LinkedHashMap<Node,RsyncResourceNode>(nodeConfigs.size()*4/3+1);
        for(AppClusterConfiguration.ResourceNodeConfiguration nodeConfig : nodeConfigs) {
            AppClusterConfiguration.RsyncResourceNodeConfiguration resyncConfig = (AppClusterConfiguration.RsyncResourceNodeConfiguration)nodeConfig;
            String nodeId = resyncConfig.getNodeId();
            Node node = cluster.getNodes().get(nodeId);
            if(node==null) throw new AppClusterConfiguration.AppClusterConfigurationException(ApplicationResources.accessor.getMessage("RsyncResource.init.nodeNotFound", getId(), nodeId));
            newResourceNodes.put(node, new RsyncResourceNode(this, node, resyncConfig));
        }
        this.delete = resourceConfiguration.isDelete();
        this.resourceNodes = Collections.unmodifiableMap(newResourceNodes);
    }

    public boolean isDelete() {
        return delete;
    }

    @Override
    public Map<Node,RsyncResourceNode> getResourceNodes() {
        return resourceNodes;
    }
}
