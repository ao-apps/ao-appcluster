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

import com.aoindustries.sql.Database;
import java.util.logging.Logger;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * Logs the activities of the application cluster to a database.
 *
 * @author  AO Industries, Inc.
 */
public class JdbcClusterLogger implements AppClusterLogger {

    private static final Logger logger = Logger.getLogger(JdbcClusterLogger.class.getName());

    private final Database database;

    public JdbcClusterLogger(String name) throws AppClusterConfiguration.AppClusterConfigurationException {
        try {
            Context ic = new InitialContext();
            Context envCtx = (Context)ic.lookup("java:comp/env");
            DataSource dataSource = (DataSource)envCtx.lookup(name);
            if(dataSource==null) throw new AppClusterConfiguration.AppClusterConfigurationException(ApplicationResources.accessor.getMessage("JdbcClusterLogger.init.datasourceNotFound", name));
            database = new Database(dataSource, logger);
        } catch(NamingException exc) {
            throw new AppClusterConfiguration.AppClusterConfigurationException(exc);
        }
    }

    @Override
    public void start() {
        // Nothing to do
    }

    @Override
    public void stop() {
        // Nothing to do
    }
}
