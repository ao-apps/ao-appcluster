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
package com.aoindustries.appcluster.csync2;

import com.aoindustries.appcluster.AppClusterConfigurationException;
import com.aoindustries.appcluster.Node;
import com.aoindustries.appcluster.ResourceNode;

/**
 * The node settings for csync2 synchronization.
 *
 * @author  AO Industries, Inc.
 */
public class Csync2ResourceNode extends ResourceNode<Csync2Resource,Csync2ResourceNode> {

    private final String exe;
    private final String config;

    protected Csync2ResourceNode(Node node, Csync2ResourceNodeConfiguration resourceNodeConfiguration) throws AppClusterConfigurationException {
        super(node, resourceNodeConfiguration);
        this.exe = resourceNodeConfiguration.getExe();
        this.config = resourceNodeConfiguration.getConfig();
    }

    /**
     * Gets the path to the csync2 executable.
     */
    public String getExe() {
        return exe;
    }

    /**
     * Gets the path to the csycn2 config file.
     */
    public String getConfig() {
        return config;
    }
}
