/*
 * ao-appcluster - Application-level clustering tools.
 * Copyright (C) 2011, 2012, 2015, 2016  AO Industries, Inc.
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

import com.aoindustries.appcluster.CronResourceSynchronizer;
import com.aoindustries.appcluster.NodeDnsStatus;
import com.aoindustries.appcluster.ResourceNodeDnsResult;
import com.aoindustries.appcluster.ResourceStatus;
import com.aoindustries.appcluster.ResourceSynchronizationMode;
import com.aoindustries.appcluster.ResourceSynchronizationResult;
import com.aoindustries.appcluster.ResourceSynchronizationResultStep;
import com.aoindustries.cron.Schedule;
import com.aoindustries.dbc.NoRowException;
import com.aoindustries.dbc.meta.Catalog;
import com.aoindustries.dbc.meta.Column;
import com.aoindustries.dbc.meta.DatabaseMetaData;
import com.aoindustries.dbc.meta.Index;
import com.aoindustries.dbc.meta.Schema;
import com.aoindustries.dbc.meta.Table;
import com.aoindustries.lang.ObjectUtils;
import com.aoindustries.sql.SQLUtility;
import com.aoindustries.util.AoArrays;
import com.aoindustries.util.ErrorPrinter;
import com.aoindustries.util.StringUtility;
import com.aoindustries.util.graph.TopologicalSorter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

/**
 * Performs synchronization using JDBC.
 * Every table must have a primary key.
 * Also, assumes that updating a non-primary key value will have no affect on other data.
 * Primary keys themselves are never updated, rows will be deleted and then inserted in this case.
 * For table dependencies, only uses primary keys and foreign keys that go to primary keys.
 * There must not be any cycle in the dependency graph.
 *
 * TODO: Verify permissions?
 * TODO: Verify indexes?
 *
 * @author  AO Industries, Inc.
 */
public class JdbcResourceSynchronizer extends CronResourceSynchronizer<JdbcResource,JdbcResourceNode> {

	private static final int FETCH_SIZE = 1000;

	protected JdbcResourceSynchronizer(JdbcResourceNode localResourceNode, JdbcResourceNode remoteResourceNode, Schedule synchronizeSchedule, Schedule testSchedule) {
		super(localResourceNode, remoteResourceNode, synchronizeSchedule, testSchedule);
	}

	/*
	 * May synchronize from a master to a slave.
	 * May test from a master to a slave or a slave to a master.
	 */
	@Override
	protected boolean canSynchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
		NodeDnsStatus localDnsStatus = localDnsResult.getNodeStatus();
		NodeDnsStatus remoteDnsStatus = remoteDnsResult.getNodeStatus();
		switch(mode) {
			case SYNCHRONIZE :
				return
					localDnsStatus==NodeDnsStatus.MASTER
					&& remoteDnsStatus==NodeDnsStatus.SLAVE
				;
			case TEST_ONLY :
				return
					(
						localDnsStatus==NodeDnsStatus.MASTER
						&& remoteDnsStatus==NodeDnsStatus.SLAVE
					) || (
						localDnsStatus==NodeDnsStatus.SLAVE
						&& remoteDnsStatus==NodeDnsStatus.MASTER
					)
				;
			default : throw new AssertionError("Unexpected mode: "+mode);
		}
	}

	/**
	 * Gets the catalog.
	 */
	private static Catalog getCatalog(DatabaseMetaData metaData) throws SQLException {
		SortedMap<String,Catalog> catalogs = metaData.getCatalogs();
		if(catalogs.isEmpty()) throw new SQLException(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.getCatalog.noRow"));
		if(catalogs.size()>1) throw new SQLException(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.getCatalog.moreThanOneRow"));
		return catalogs.get(catalogs.firstKey());
	}

	@Override
	protected ResourceSynchronizationResult synchronize(ResourceSynchronizationMode mode, ResourceNodeDnsResult localDnsResult, ResourceNodeDnsResult remoteDnsResult) {
		final JdbcResource resource = localResourceNode.getResource();

		List<ResourceSynchronizationResultStep> steps = new ArrayList<>();

		// If exception is not caught within its own step, the error will be added with this step detail.
		// Each step should update this.
		long stepStartTime = System.currentTimeMillis();
		String step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.connect");
		StringBuilder stepOutput = new StringBuilder();
		StringBuilder stepWarning = new StringBuilder();
		StringBuilder stepError = new StringBuilder();

		try {
			// Will always synchronize or test from master to slave
			String fromDataSourceName;
			String toDataSourceName;
			NodeDnsStatus localDnsStatus = localDnsResult.getNodeStatus();
			NodeDnsStatus remoteDnsStatus = remoteDnsResult.getNodeStatus();
			switch(mode) {
				case SYNCHRONIZE :
					if(
						localDnsStatus==NodeDnsStatus.MASTER
						&& remoteDnsStatus==NodeDnsStatus.SLAVE
					) {
						fromDataSourceName = localResourceNode.getDataSource();
						toDataSourceName = remoteResourceNode.getDataSource();
					} else {
						throw new AssertionError();
					}
					break;
				case TEST_ONLY :
					if(
						localDnsStatus==NodeDnsStatus.MASTER
						&& remoteDnsStatus==NodeDnsStatus.SLAVE
					) {
						fromDataSourceName = localResourceNode.getDataSource();
						toDataSourceName = remoteResourceNode.getDataSource();
					} else if(
						localDnsStatus==NodeDnsStatus.SLAVE
						&& remoteDnsStatus==NodeDnsStatus.MASTER
					) {
						fromDataSourceName = remoteResourceNode.getDataSource();
						toDataSourceName = localResourceNode.getDataSource();
					} else {
						throw new AssertionError();
					}
					break;
				default : throw new AssertionError("Unexpected mode: "+mode);
			}
			stepOutput.append("fromDataSourceName: ").append(fromDataSourceName).append('\n');
			stepOutput.append("toDataSourceName..: ").append(toDataSourceName).append('\n');

			// Lookup the data sources
			Context ic = new InitialContext();
			Context envCtx = (Context)ic.lookup("java:comp/env");
			DataSource fromDataSource = (DataSource)envCtx.lookup(fromDataSourceName);
			if(fromDataSource==null) throw new NullPointerException("fromDataSource is null");
			DataSource toDataSource = (DataSource)envCtx.lookup(toDataSourceName);
			if(toDataSource==null) throw new NullPointerException("toDataSource is null");

			// Step #1: Connect to the data sources
			Connection fromConn = fromDataSource.getConnection();
			try {
				stepOutput.append("fromConn..........: ").append(fromConn).append('\n');
				fromConn.setReadOnly(true);
				fromConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
				fromConn.setAutoCommit(false);

				Connection toConn = toDataSource.getConnection();
				try {
					stepOutput.append("toConn............: ").append(toConn).append('\n');
					toConn.setReadOnly(mode!=ResourceSynchronizationMode.SYNCHRONIZE);
					toConn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
					toConn.setAutoCommit(false);

					// Connection successful
					steps.add(new ResourceSynchronizationResultStep(stepStartTime, System.currentTimeMillis(), ResourceStatus.HEALTHY, step, stepOutput, stepWarning, stepError));

					// Step #2 Compare meta data
					stepStartTime = System.currentTimeMillis();
					step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.compareMetaData");
					stepOutput.setLength(0);
					stepWarning.setLength(0);
					stepError.setLength(0);

					Catalog fromCatalog = getCatalog(new DatabaseMetaData(fromConn));
					Catalog toCatalog = getCatalog(new DatabaseMetaData(toConn));

					final Set<String> schemas = resource.getSchemas();
					final Set<String> tableTypes = resource.getTableTypes();
					final Set<String> excludeTables = resource.getExcludeTables();
					compareSchemas(fromCatalog, toCatalog, schemas, tableTypes, excludeTables, stepError);

					steps.add(
						new ResourceSynchronizationResultStep(
							stepStartTime,
							System.currentTimeMillis(),
							stepError.length()!=0 ? ResourceStatus.ERROR
							: stepWarning.length()!=0 ? ResourceStatus.WARNING
							: ResourceStatus.HEALTHY,
							step,
							stepOutput,
							stepWarning,
							stepError
						)
					);

					// Only continue if all meta data is compatible
					if(stepError.length()==0) {
						if(mode==ResourceSynchronizationMode.TEST_ONLY) {
							stepStartTime = System.currentTimeMillis();
							step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.compareData");
							stepOutput.setLength(0);
							stepWarning.setLength(0);
							stepError.setLength(0);

							testSchemasData(fromConn, toConn, resource.getTestTimeout(), fromCatalog, toCatalog, schemas, tableTypes, excludeTables, resource.getNoWarnTables(), stepOutput, stepWarning);
							steps.add(
								new ResourceSynchronizationResultStep(
									stepStartTime,
									System.currentTimeMillis(),
									stepError.length()!=0 ? ResourceStatus.ERROR
									: stepWarning.length()!=0 ? ResourceStatus.WARNING
									: ResourceStatus.HEALTHY,
									step,
									stepOutput,
									stepWarning,
									stepError
								)
							);

							// Nothing should have been changed, roll-back just to be safe
							toConn.rollback();
						} else if(mode==ResourceSynchronizationMode.SYNCHRONIZE) {
							// Run any preparation steps
							boolean hasError = false;
							for(Map.Entry<String,String> prepareSlave : resource.getPrepareSlaves().entrySet()) {
								stepStartTime = System.currentTimeMillis();
								step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.prepareSlave", prepareSlave.getKey());
								stepOutput.setLength(0);
								stepWarning.setLength(0);
								stepError.setLength(0);
								try {
									try (Statement stmt = toConn.createStatement()) {
										int updateCount = stmt.executeUpdate(prepareSlave.getValue());
										stepOutput.append(
											ApplicationResources.accessor.getMessage(
												"JdbcResourceSynchronizer.synchronize.step.prepareSlave.updateCount",
												prepareSlave.getKey(),
												updateCount
											)
										);
									}
								} catch(SQLException e) {
									ErrorPrinter.printStackTraces(e, stepError);
								}
								steps.add(
									new ResourceSynchronizationResultStep(
										stepStartTime,
										System.currentTimeMillis(),
										stepError.length()!=0 ? ResourceStatus.ERROR
										: stepWarning.length()!=0 ? ResourceStatus.WARNING
										: ResourceStatus.HEALTHY,
										step,
										stepOutput,
										stepWarning,
										stepError
									)
								);
								if(stepError.length()>0) {
									hasError = true;
									break;
								}
							}
							if(!hasError) {
								stepStartTime = System.currentTimeMillis();
								step = ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronize.step.synchronizeData");
								stepOutput.setLength(0);
								stepWarning.setLength(0);
								stepError.setLength(0);

								synchronizeData(fromConn, toConn, resource.getSynchronizeTimeout(), fromCatalog, schemas, tableTypes, excludeTables, stepOutput);
								steps.add(
									new ResourceSynchronizationResultStep(
										stepStartTime,
										System.currentTimeMillis(),
										stepError.length()!=0 ? ResourceStatus.ERROR
										: stepWarning.length()!=0 ? ResourceStatus.WARNING
										: ResourceStatus.HEALTHY,
										step,
										stepOutput,
										stepWarning,
										stepError
									)
								);

								// Commit/rollback based on errors
								if(stepError.length()==0) toConn.commit();
								else toConn.rollback();
							} else {
								toConn.rollback();
							}
						} else {
							throw new AssertionError("Unexpected mode: "+mode);
						}
					} else {
						toConn.rollback();
					}
				} catch(Throwable T) {
					toConn.rollback();
					throw T;
				} finally {
					toConn.setAutoCommit(true);
					toConn.close();
				}
			} finally {
				fromConn.rollback(); // Is read-only, this should always be OK and preferred to commit of accidental changes
				fromConn.setAutoCommit(true);
				fromConn.close();
			}
		} catch(ThreadDeath TD) {
			throw TD;
		} catch(Throwable T) {
			ErrorPrinter.printStackTraces(T, stepError);
			stepError.append('\n');
			steps.add(
				new ResourceSynchronizationResultStep(
					stepStartTime,
					System.currentTimeMillis(),
					ResourceStatus.ERROR,
					step,
					stepOutput,
					stepWarning,
					stepError
				)
			);
		}

		return new ResourceSynchronizationResult(
			localResourceNode,
			remoteResourceNode,
			mode,
			steps
		);
	}

	private static void compareSchemas(
		Catalog fromCatalog,
		Catalog toCatalog,
		Set<String> schemas,
		Set<String> tableTypes,
		Set<String> excludeTables,
		StringBuilder stepError
	) throws SQLException {
		for(String schema : schemas) {
			compareSchema(fromCatalog.getSchema(schema), toCatalog.getSchema(schema), tableTypes, excludeTables, stepError);
		}
	}

	private static void compareSchema(
		Schema fromSchema,
		Schema toSchema,
		Set<String> tableTypes,
		Set<String> excludeTables,
		StringBuilder stepError
	) throws SQLException {
		SortedMap<String,Table> fromTables = fromSchema.getTables();
		SortedMap<String,Table> toTables = toSchema.getTables();

		// Get the union of all table names of included types that are not excluded
		SortedSet<String> allTableNames = new TreeSet<>(DatabaseMetaData.getCollator());
		for(Table table : fromTables.values()) {
			String tableName = table.getName();
			if(
				!excludeTables.contains(fromSchema.getName()+'.'+tableName)
				&& tableTypes.contains(table.getTableType())
			) allTableNames.add(tableName);
		}
		for(Table table : toTables.values()) {
			String tableName = table.getName();
			if(
				!excludeTables.contains(toSchema.getName()+'.'+tableName)
				&& tableTypes.contains(table.getTableType())
			) allTableNames.add(tableName);
		}

		for(String tableName : allTableNames) {
			Table fromTable = fromTables.get(tableName);
			Table toTable = toTables.get(tableName);
			if(fromTable!=null) {
				if(toTable!=null) {
					// Exists in both, continue on to check columns
					compareTable(fromTable, toTable, stepError);
				} else {
					stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareSchema.missingTable", toSchema.getCatalog(), toSchema, tableName)).append('\n');
				}
			} else {
				if(toTable!=null) {
					stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareSchema.extraTable", toSchema.getCatalog(), toSchema, tableName)).append('\n');
				} else {
					throw new AssertionError("Must exist in at least one of the sets");
				}
			}
		}
	}

	private static void compareTable(Table fromTable, Table toTable, StringBuilder stepError) throws SQLException {
		assert fromTable.equals(toTable);
		if(!fromTable.getTableType().equals(toTable.getTableType())) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareTable.mismatchedType",
					fromTable.getSchema(),
					fromTable,
					fromTable.getTableType(),
					toTable.getTableType()
				)
			).append('\n');
		} else {
			// Compare columns
			SortedMap<String,Column> fromColumns = fromTable.getColumnMap();
			SortedMap<String,Column> toColumns = toTable.getColumnMap();
			SortedSet<String> allColumnNames = new TreeSet<>(DatabaseMetaData.getCollator());
			allColumnNames.addAll(fromColumns.keySet());
			allColumnNames.addAll(toColumns.keySet());
			for(String columnName : allColumnNames) {
				Column fromColumn = fromColumns.get(columnName);
				Column toColumn = toColumns.get(columnName);
				if(fromColumn!=null) {
					if(toColumn!=null) {
						// Exists in both, continue on to check column detail
						compareColumn(fromColumn, toColumn, stepError);
					} else {
						stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareTable.missingColumn", toTable.getSchema().getCatalog(), toTable.getSchema(), toTable, columnName)).append('\n');
					}
				} else {
					if(toColumn!=null) {
						stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareTable.extraColumn", toTable.getSchema().getCatalog(), toTable.getSchema(), toTable, columnName)).append('\n');
					} else {
						throw new AssertionError("Must exist in at least one of the sets");
					}
				}
			}
			// Compare primary key
			comparePrimaryKey(fromTable, toTable, fromTable.getPrimaryKey(), toTable.getPrimaryKey(), stepError);
			// Compare foreign keys
			//stepError.append(fromTable.getSchema()).append('.').append(fromTable.getName()).append('\n');
			Set<? extends Table> fromConnected = fromTable.getImportedTables();
			//stepError.append("    fromConnected....: ").append(fromConnected).append('\n');
			Set<? extends Table> toConnected = toTable.getImportedTables();
			//stepError.append("    toConnected......: ").append(toConnected).append('\n');
			if(!fromConnected.equals(toConnected)) {
				stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareTable.mismatchedConnectedVertices", fromTable.getSchema(), fromTable, fromConnected, toConnected)).append('\n');
			}
			Set<? extends Table> fromBackConnected = fromTable.getExportedTables();
			//stepError.append("    fromBackConnected: ").append(fromBackConnected).append('\n');
			Set<? extends Table> toBackConnected = toTable.getExportedTables();
			//stepError.append("    toBackConnected..: ").append(toBackConnected).append('\n');
			if(!fromBackConnected.equals(toBackConnected)) {
				stepError.append(ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.compareTable.mismatchedBackConnectedVertices", fromTable.getSchema(), fromTable, fromBackConnected, toBackConnected)).append('\n');
			}
		}
	}

	private static void compareColumn(Column fromColumn, Column toColumn, StringBuilder stepError) {
		String schema = fromColumn.getTable().getSchema().getName();
		String table = fromColumn.getTable().getName();
		String column = fromColumn.getName();
		// int typeName
		if(fromColumn.getDataType()!=toColumn.getDataType()) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.dataType",
					schema,
					table,
					column,
					fromColumn.getDataType(),
					toColumn.getDataType()
				)
			).append('\n');
		}
		// String typeName
		if(!ObjectUtils.equals(fromColumn.getTypeName(), toColumn.getTypeName())) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.typeName",
					schema,
					table,
					column,
					fromColumn.getTypeName(),
					toColumn.getTypeName()
				)
			).append('\n');
		}
		// Integer columnSize
		if(!ObjectUtils.equals(fromColumn.getColumnSize(), toColumn.getColumnSize())) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.columnSize",
					schema,
					table,
					column,
					fromColumn.getColumnSize(),
					toColumn.getColumnSize()
				)
			).append('\n');
		}
		// Integer decimalDigits
		if(!ObjectUtils.equals(fromColumn.getDecimalDigits(), toColumn.getDecimalDigits())) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.decimalDigits",
					schema,
					table,
					column,
					fromColumn.getDecimalDigits(),
					toColumn.getDecimalDigits()
				)
			).append('\n');
		}
		// int nullable
		if(fromColumn.getNullable()!=toColumn.getNullable()) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.nullable",
					schema,
					table,
					column,
					fromColumn.getNullable(),
					toColumn.getNullable()
				)
			).append('\n');
		}
		// String columnDef
		if(!ObjectUtils.equals(fromColumn.getColumnDef(), toColumn.getColumnDef())) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.columnDef",
					schema,
					table,
					column,
					fromColumn.getColumnDef(),
					toColumn.getColumnDef()
				)
			).append('\n');
		}
		// Integer charOctetLength
		if(!ObjectUtils.equals(fromColumn.getCharOctetLength(), toColumn.getCharOctetLength())) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.charOctetLength",
					schema,
					table,
					column,
					fromColumn.getCharOctetLength(),
					toColumn.getCharOctetLength()
				)
			).append('\n');
		}
		// int ordinalPosition
		if(fromColumn.getOrdinalPosition()!=toColumn.getOrdinalPosition()) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.ordinalPosition",
					schema,
					table,
					column,
					fromColumn.getOrdinalPosition(),
					toColumn.getOrdinalPosition()
				)
			).append('\n');
		}
		// String isNullable
		if(!ObjectUtils.equals(fromColumn.getIsNullable(), toColumn.getIsNullable())) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.isNullable",
					schema,
					table,
					column,
					fromColumn.getIsNullable(),
					toColumn.getIsNullable()
				)
			).append('\n');
		}
		// String isAutoincrement
		if(!ObjectUtils.equals(fromColumn.getIsAutoincrement(), toColumn.getIsAutoincrement())) {
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.compareColumn.mismatch.isAutoincrement",
					schema,
					table,
					column,
					fromColumn.getIsAutoincrement(),
					toColumn.getIsAutoincrement()
				)
			).append('\n');
		}
	}

	private static void comparePrimaryKey(Table fromTable, Table toTable, Index fromPrimaryKey, Index toPrimaryKey, StringBuilder stepError) {
		if(fromPrimaryKey==null) {
			Schema fromSchema = fromTable.getSchema();
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.comparePrimaryKey.primaryKeyMissing",
					fromSchema.getCatalog(),
					fromSchema,
					fromTable
				)
			).append('\n');
		}
		if(toPrimaryKey==null) {
			Schema toSchema = toTable.getSchema();
			stepError.append(
				ApplicationResources.accessor.getMessage(
					"JdbcResourceSynchronizer.comparePrimaryKey.primaryKeyMissing",
					toSchema.getCatalog(),
					toSchema,
					toTable
				)
			).append('\n');
		}
		if(fromPrimaryKey!=null && toPrimaryKey!=null) {
			if(!ObjectUtils.equals(fromPrimaryKey.getName(), toPrimaryKey.getName())) {
				stepError.append(
					ApplicationResources.accessor.getMessage(
						"JdbcResourceSynchronizer.comparePrimaryKey.mismatch.name",
						fromTable.getSchema(),
						fromTable,
						fromPrimaryKey.getName(),
						toPrimaryKey.getName()
					)
				).append('\n');
			}
			if(!fromPrimaryKey.getColumns().equals(toPrimaryKey.getColumns())) {
				stepError.append(
					ApplicationResources.accessor.getMessage(
						"JdbcResourceSynchronizer.comparePrimaryKey.mismatch.columns",
						fromTable.getSchema(),
						fromTable,
						"(" + StringUtility.join(fromPrimaryKey.getColumns(), ", ") + ")",
						"(" + StringUtility.join(toPrimaryKey.getColumns(), ", ") + ")"
					)
				).append('\n');
			}
		}
	}

	private static void testSchemasData(Connection fromConn, Connection toConn, int timeout, Catalog fromCatalog, Catalog toCatalog, Set<String> schemas, Set<String> tableTypes, Set<String> excludeTables, Set<String> noWarnTables, StringBuilder stepOutput, StringBuilder stepWarning) throws SQLException {
		List<Object> outputTable = new ArrayList<>();
		try {
			for(String schema : schemas) {
				testSchemaData(fromConn, toConn, timeout, fromCatalog.getSchema(schema), toCatalog.getSchema(schema), tableTypes, excludeTables, noWarnTables, outputTable, stepOutput, stepWarning);
			}
		} finally {
			try {
				// Insert the table before any other output
				String currentOut = stepOutput.toString();
				stepOutput.setLength(0);
				SQLUtility.printTable(
					new String[] {
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.schema"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.table"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.matches"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.modified"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.missing"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.testSchemasData.column.extra"),
					},
					outputTable.toArray(),
					stepOutput,
					true,
					new boolean[] {
						false,
						false,
						true,
						true,
						true,
						true
					}
				);
				stepOutput.append(currentOut);
			} catch(IOException exc) {
				throw new AssertionError(exc);
			}
		}
	}

	private static void testSchemaData(
		Connection fromConn,
		Connection toConn,
		int timeout,
		Schema fromSchema,
		Schema toSchema,
		Set<String> tableTypes,
		Set<String> excludeTables,
		Set<String> noWarnTables,
		List<Object> outputTable,
		StringBuilder stepOutput,
		StringBuilder stepWarning
	) throws SQLException {
		assert fromSchema.equals(toSchema);

		SortedMap<String,Table> fromTables = fromSchema.getTables();
		SortedMap<String,Table> toTables = toSchema.getTables();

		assert fromTables.keySet().equals(toTables.keySet()) : "This should have been caught by the meta data checks";

		for(String tableName : fromTables.keySet()) {
			Table fromTable = fromTables.get(tableName);
			Table toTable = toTables.get(tableName);
			String tableType = fromTable.getTableType();
			assert tableType.equals(toTable.getTableType()) : "This should have been caught by the meta data checks";
			if(
				!excludeTables.contains(fromSchema.getName()+'.'+tableName)
				&& tableTypes.contains(tableType)
			) {
				if("TABLE".equals(tableType)) testTableData(fromConn, toConn, timeout, fromTable, toTable, noWarnTables, outputTable, stepOutput, stepWarning);
				else throw new SQLException("Unimplemented table type: " + tableType);
			}
		}
	}

	/**
	 * Appends a value in a per-type way.
	 */
	private static void appendValue(StringBuilder sb, int dataType, Object value) {
		if(value==null) {
			sb.append("NULL");
		} else {
			switch(dataType) {
				// Types without quotes
				case Types.BIGINT :
				case Types.BIT :
				case Types.BOOLEAN :
				case Types.DOUBLE :
				case Types.FLOAT :
				case Types.INTEGER :
				case Types.NULL :
				case Types.REAL :
				case Types.SMALLINT :
				case Types.TINYINT :
					sb.append(value.toString());
					break;
				// All other types will use quotes
				default :
					try {
						sb.append('\'');
						StringUtility.replace(value.toString(), "'", "''", sb);
						sb.append('\'');
					} catch(IOException exc) {
						throw new AssertionError(exc); // Should not happen
					}
			}
		}
	}

	/**
	 * A row encapsulates one set of results.
	 */
	static class Row implements Comparable<Row> {

		private final Column[] primaryKeyColumns;
		private final Column[] nonPrimaryKeyColumns;
		private final Object[] values;

		Row(Column[] primaryKeyColumns, Column[] nonPrimaryKeyColumns, Object[] values) {
			this.primaryKeyColumns = primaryKeyColumns;
			this.nonPrimaryKeyColumns = nonPrimaryKeyColumns;
			this.values = values;
		}

		/**
		 * Orders rows by the values of the primary key columns.
		 * Orders rows in the same exact way as the underlying database to make
		 * sure that the one-pass comparison is correct.
		 * Only returns zero when the primary key values are an exact match.
		 * The comparison is consistent with equals for all primary key values.
		 */
		@Override
		public int compareTo(Row other) {
			for(Column primaryKeyColumn : primaryKeyColumns) {
				int index = primaryKeyColumn.getOrdinalPosition() - 1;
				Object val = values[index];
				Object otherVal = other.values[index];
				int diff;
				int dataType = primaryKeyColumn.getDataType();
				try {
					switch(dataType) {
						case Types.BIGINT :
							diff = ((Long)val).compareTo((Long)otherVal);
							break;
						// These were converted to UTF8 byte[] during order by.
						// Use the same conversion here
						case Types.CHAR :
						case Types.VARCHAR :
							try {
								//diff = ((String)val).compareTo((String)otherVal);
								diff = AoArrays.compare(
									((String)val).getBytes("UTF-8"),
									((String)otherVal).getBytes("UTF-8")
								);
							} catch(UnsupportedEncodingException exc) {
								throw new RuntimeException(exc);
							}
							break;
						case Types.DATE :
							diff = ((Date)val).compareTo((Date)otherVal);
							break;
						case Types.DECIMAL :
						case Types.NUMERIC :
							diff = ((BigDecimal)val).compareTo((BigDecimal)otherVal);
							break;
						case Types.DOUBLE :
							diff = ((Double)val).compareTo((Double)otherVal);
							break;
						case Types.FLOAT :
							diff = ((Float)val).compareTo((Float)otherVal);
							break;
						case Types.SMALLINT :
						case Types.INTEGER :
							diff = ((Integer)val).compareTo((Integer)otherVal);
							break;
						case Types.TIME :
							diff = ((Time)val).compareTo((Time)otherVal);
							break;
						case Types.TIMESTAMP :
							diff = ((Timestamp)val).compareTo((Timestamp)otherVal);
							break;
						default : throw new UnsupportedOperationException("Type comparison not implemented: "+primaryKeyColumn.getDataType());
					}
				} catch(ClassCastException e) {
					ClassCastException newE = new ClassCastException(e.getMessage()+": dataType="+dataType+", otherVal.class.name="+otherVal.getClass().getName());
					newE.initCause(e);
					throw newE;
				}
				assert (diff==0) == (val.equals(otherVal)) : "Not consistent with equals: val="+val+", otherVal="+otherVal;
				if(diff!=0) return diff;
			}
			return 0; // Exact match
		}

		boolean equalsNonPrimaryKey(Row other) {
			for(Column nonPrimaryKeyColumn : nonPrimaryKeyColumns) {
				int index = nonPrimaryKeyColumn.getOrdinalPosition() - 1;
				if(
					!ObjectUtils.equals(
						values[index],
						other.values[index]
					)
				) return false;
			}
			return true;
		}

		/**
		 * Gets a string representation of the primary key values of this row.
		 * This is only meant to be human readable, not for sending to SQL directly.
		 */
		String getPrimaryKeyValues() {
			StringBuilder sb = new StringBuilder();
			sb.append('(');
			boolean didOne = false;
			for(Column primaryKeyColumn : primaryKeyColumns) {
				if(didOne) sb.append(", ");
				else didOne = true;
				appendValue(sb, primaryKeyColumn.getDataType(), values[primaryKeyColumn.getOrdinalPosition()-1]);
			}
			sb.append(')');
			return sb.toString();
		}
	}

	private static Column[] getNonPrimaryKeyColumns(List<Column> columns, List<Column> pkColumns) {
		Column[] nonPrimaryKeyColumns = new Column[columns.size() - pkColumns.size()];
		int index = 0;
		for(Column column : columns) {
			if(!pkColumns.contains(column)) nonPrimaryKeyColumns[index++] = column;
		}
		if(index!=nonPrimaryKeyColumns.length) throw new AssertionError();
		return nonPrimaryKeyColumns;
	}

	/**
	 * Iterates rows from a result set, ensuring that each row is properly ordered after the previous.
	 */
	static class RowIterator {
		private final Column[] columns;
		private final Column[] primaryKeyColumns;
		private final Column[] nonPrimaryKeyColumns;
		private final ResultSet results;
		private Row previousRow;
		private Row nextRow;

		RowIterator(List<Column> columns, Index primaryKey, ResultSet results) throws SQLException {
			this.columns = columns.toArray(new Column[columns.size()]);
			List<Column> pkColumns = primaryKey.getColumns();
			this.primaryKeyColumns = pkColumns.toArray(new Column[pkColumns.size()]);
			this.nonPrimaryKeyColumns = getNonPrimaryKeyColumns(columns, pkColumns);
			this.results = results;
			this.nextRow = getNextRow();
		}

		/**
		 * Gest the next row from the results.
		 */
		private Row getNextRow() throws SQLException {
			if(results.next()) {
				Object[] values = new Object[columns.length];
				for(int index=0; index<values.length; index++) values[index] = results.getObject(index+1);
				return new Row(primaryKeyColumns, nonPrimaryKeyColumns, values);
			} else {
				return null;
			}
		}

		/**
		 * Gets and does not remove the next row.
		 *
		 * @return  the next row or <code>null</code> when all rows have been read.
		 */
		Row peek() {
			return nextRow;
		}

		/**
		 * Gets and removes the next row.
		 *
		 * @return  the next row or <code>null</code> when all rows have been read.
		 */
		Row remove() throws SQLException {
			if(nextRow==null) return nextRow;
			Row newNextRow = getNextRow();
			if(newNextRow==null) {
				previousRow = null;
				nextRow = null;
				return null;
			}
			if(previousRow!=null) {
				// Make sure this row is after the previous
				if(newNextRow.compareTo(previousRow)<=0) throw new SQLException("Rows out of order: " + previousRow.getPrimaryKeyValues() + " and " + newNextRow.getPrimaryKeyValues());
			}
			previousRow = nextRow;
			nextRow = newNextRow;
			return newNextRow;
		}
	}

	/**
	 * Gets the SQL query used to select the entire table (except with binary data changed to md5 hashes) in primary key order.
	 */
	private static String getSelectSql(Table table) throws SQLException {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		boolean didOne = false;
		for(Column column : table.getColumns()) {
			if(didOne) sql.append(", ");
			else didOne = true;
			switch(column.getDataType()) {
				// These will be verified using md5
				case Types.BINARY :
				case Types.BLOB :
				case Types.LONGVARBINARY :
				case Types.VARBINARY :
					sql.append(" md5(\"").append(column.getName()).append("\")");
					break;
				// All others are fully compared
				default :
					sql.append('"').append(column.getName()).append('"');
			}
		}
		sql.append(" FROM \"").append(table.getSchema().getName()).append("\".\"").append(table.getName()).append("\" ORDER BY ");
		didOne = false;
		for(Column column : table.getPrimaryKey().getColumns()) {
			if(didOne) sql.append(", ");
			else didOne = true;
			switch(column.getDataType()) {
				// These will be verified using md5
				case Types.BINARY :
				case Types.BLOB :
				case Types.LONGVARBINARY :
				case Types.VARBINARY :
					throw new SQLException("Type not supported in primary key: "+column.getDataType());
				// These will be converted to UTF8 bytea for collator-neutral ordering (not dependent on PostgreSQL lc_collate setting)
				case Types.CHAR :
				case Types.VARCHAR :
					sql.append("convert_to(\"").append(column.getName()).append("\", 'UTF8')");
					break;
				// All others are compared directly
				default :
					sql.append('"').append(column.getName()).append('"');
			}
		}
		return sql.toString();
	}

	/**
	 * Queries both from and to tables, sorted by each column of the primary key in ascending order.
	 * All differences are found in a single pass through the tables, with no buffering and only a single query of each result.
	 */
	private static void testTableData(
		Connection fromConn,
		Connection toConn,
		int timeout,
		Table fromTable,
		Table toTable,
		Set<String> noWarnTables,
		List<Object> outputTable,
		StringBuilder stepOutput,
		StringBuilder stepWarning
	) throws SQLException {
		assert fromTable.equals(toTable);
		final String schema = fromTable.getSchema().getName();
		final StringBuilder stepResults = noWarnTables.contains(schema+'.'+fromTable.getName()) ? stepOutput : stepWarning;
		final List<Column> columns = fromTable.getColumns();
		final Index primaryKey = fromTable.getPrimaryKey();
		final String sql = getSelectSql(fromTable);
		try (Statement fromStmt = fromConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
			fromStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
			fromStmt.setFetchSize(FETCH_SIZE);
			try (Statement toStmt = toConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
				toStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
				toStmt.setFetchSize(FETCH_SIZE);
				try (
					ResultSet fromResults = fromStmt.executeQuery(sql);
					ResultSet toResults = toStmt.executeQuery(sql)
				) {
					long matches = 0;
					long modified = 0;
					long missing = 0;
					long extra = 0;
					RowIterator fromIter = new RowIterator(columns, primaryKey, fromResults);
					RowIterator toIter = new RowIterator(columns, primaryKey, toResults);
					while(true) {
						Row fromRow = fromIter.peek();
						Row toRow = toIter.peek();
						if(fromRow!=null) {
							if(toRow!=null) {
								int primaryKeyDiff = fromRow.compareTo(toRow);
								if(primaryKeyDiff==0) {
									// Primary keys have already been compared and are known to be equal, only need to compare the remaining columns
									if(fromRow.equalsNonPrimaryKey(toRow)) {
										// Exact match, remove both
										matches++;
									} else {
										// Modified
										stepResults.append(
											ApplicationResources.accessor.getMessage(
												"JdbcResourceSynchronizer.testTableData.modified",
												schema,
												fromTable,
												fromRow.getPrimaryKeyValues()
											)
										).append('\n');
										modified++;
									}
									fromIter.remove();
									toIter.remove();
								} else if(primaryKeyDiff<0) {
									// Missing
									stepResults.append(
										ApplicationResources.accessor.getMessage(
											"JdbcResourceSynchronizer.testTableData.missing",
											schema,
											fromTable,
											fromRow.getPrimaryKeyValues()
										)
									).append('\n');
									missing++;
									fromIter.remove();
								} else {
									assert primaryKeyDiff>0;
									// Extra
									stepResults.append(
										ApplicationResources.accessor.getMessage(
											"JdbcResourceSynchronizer.testTableData.extra",
											schema,
											toTable,
											toRow.getPrimaryKeyValues()
										)
									).append('\n');
									extra++;
									toIter.remove();
								}
							} else {
								// Missing
								stepResults.append(
									ApplicationResources.accessor.getMessage(
										"JdbcResourceSynchronizer.testTableData.missing",
										schema,
										fromTable,
										fromRow.getPrimaryKeyValues()
									)
								).append('\n');
								missing++;
								fromIter.remove();
							}
						} else {
							if(toRow!=null) {
								// Extra
								stepResults.append(
									ApplicationResources.accessor.getMessage(
										"JdbcResourceSynchronizer.testTableData.extra",
										schema,
										toTable,
										toRow.getPrimaryKeyValues()
									)
								).append('\n');
								extra++;
								toIter.remove();
							} else {
								// All rows done
								break;
							}
						}
					}
					outputTable.add(schema);
					outputTable.add(fromTable.getName());
					outputTable.add(matches);
					outputTable.add(modified==0 ? null : modified);
					outputTable.add(missing==0 ? null : missing);
					outputTable.add(extra==0 ? null : extra);
				}
			}
		}
	}

	private static void synchronizeData(Connection fromConn, Connection toConn, int synchronizeTimeout, Catalog catalog, Set<String> schemas, Set<String> tableTypes, Set<String> excludeTables, StringBuilder stepOutput) throws SQLException {
		// Find the set of tables that will be synchronized
		Set<Table> tables = new LinkedHashSet<>();
		for(String schemaName : schemas) {
			Schema schema = catalog.getSchema(schemaName);
			for(Table table : schema.getTables().values()) {
				if(
					!excludeTables.contains(schema.getName()+'.'+table.getName())
					&& tableTypes.contains(table.getTableType())
				) {
					tables.add(table);
				}
			}
		}

		Map<Table,Long> matches = new HashMap<>();
		Map<Table,Long> updates = new HashMap<>();
		Map<Table,Long> inserts = new HashMap<>();
		Map<Table,Long> deletes = new HashMap<>();
		try {
			// Topological sort based on foreign key dependencies
			List<Table> sortedTables = new ArrayList<>(new TopologicalSorter<>(catalog.getForeignKeyGraph(tableTypes), true).sortGraph());
			//stepOutput.append("sortedTables=").append(sortedTables).append('\n');
			sortedTables.retainAll(tables);
			//stepOutput.append("sortedTables=").append(sortedTables).append('\n');

			// Keep counts from the delete pass to help avoid unnecessary second scans
			Map<Table,Long> modifieds = new HashMap<>();
			Map<Table,Long> missings = new HashMap<>();

			// Delete extra rows from each table backwards
			for(int i=sortedTables.size()-1; i>=0; i--) {
				deleteExtraRows(fromConn, toConn, synchronizeTimeout, sortedTables.get(i), stepOutput, matches, modifieds, missings, deletes);
			}

			// Update/insert forwards
			for(Table table : sortedTables) {
				if(modifieds.get(table)>0 || missings.get(table)>0) {
					updateAndInsertRows(fromConn, toConn, synchronizeTimeout, table, stepOutput, matches, modifieds, missings, updates, inserts);
				}
			}
		} finally {
			List<Object> outputTable = new ArrayList<>();
			for(Table table : tables) {
				Long update = updates.get(table);
				Long insert = inserts.get(table);
				Long delete = deletes.get(table);
				outputTable.add(table.getSchema().getName());
				outputTable.add(table.getName());
				outputTable.add(matches.get(table));
				outputTable.add(update==null || update==0 ? null : update);
				outputTable.add(insert==null || insert==0 ? null : insert);
				outputTable.add(delete==null || delete==0 ? null : delete);
			}
			try {
				// Insert the table before any other output
				String currentOut = stepOutput.toString();
				stepOutput.setLength(0);
				SQLUtility.printTable(
					new String[] {
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronizeData.column.schema"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronizeData.column.table"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronizeData.column.matches"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronizeData.column.update"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronizeData.column.insert"),
						ApplicationResources.accessor.getMessage("JdbcResourceSynchronizer.synchronizeData.column.delete"),
					},
					outputTable.toArray(),
					stepOutput,
					true,
					new boolean[] {
						false,
						false,
						true,
						true,
						true,
						true
					}
				);
				stepOutput.append(currentOut);
			} catch(IOException exc) {
				throw new AssertionError(exc);
			}
		}
	}

	/**
	 * Deletes the extra rows for this table.
	 * Also sets the number of matching, missing, and modified rows to help avoid unnecessary second scans.
	 */
	private static void deleteExtraRows(
		Connection fromConn,
		Connection toConn,
		int timeout,
		Table table,
		StringBuilder stepOutput,
		Map<Table,Long> matchesMap,
		Map<Table,Long> modifiedsMap,
		Map<Table,Long> missingsMap,
		Map<Table,Long> deletesMap
	) throws SQLException {
		final String schema = table.getSchema().getName();
		final List<Column> columns = table.getColumns();
		final Index primaryKey = table.getPrimaryKey();
		// Find rows to delete
		List<Row> deleteRows = new ArrayList<>();
		String selectSql = getSelectSql(table);
		try (Statement fromStmt = fromConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
			fromStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
			fromStmt.setFetchSize(FETCH_SIZE);
			try (Statement toStmt = toConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
				toStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
				toStmt.setFetchSize(FETCH_SIZE);
				try (
					ResultSet fromResults = fromStmt.executeQuery(selectSql);
					ResultSet toResults = toStmt.executeQuery(selectSql)
				) {
					long matches = 0;
					long modified = 0;
					long missing = 0;
					RowIterator fromIter = new RowIterator(columns, primaryKey, fromResults);
					RowIterator toIter = new RowIterator(columns, primaryKey, toResults);
					while(true) {
						Row fromRow = fromIter.peek();
						Row toRow = toIter.peek();
						if(fromRow!=null) {
							if(toRow!=null) {
								int primaryKeyDiff = fromRow.compareTo(toRow);
								if(primaryKeyDiff==0) {
									// Primary keys have already been compared and are known to be equal, only need to compare the remaining columns
									if(fromRow.equalsNonPrimaryKey(toRow)) {
										// Exact match, remove both
										matches++;
									} else {
										modified++;
									}
									fromIter.remove();
									toIter.remove();
								} else if(primaryKeyDiff<0) {
									// Missing
									missing++;
									fromIter.remove();
								} else {
									assert primaryKeyDiff>0;
									// Extra
									deleteRows.add(toRow);
									toIter.remove();
								}
							} else {
								// Missing
								missing++;
								fromIter.remove();
							}
						} else {
							if(toRow!=null) {
								// Extra
								deleteRows.add(toRow);
								toIter.remove();
							} else {
								// All rows done
								break;
							}
						}
					}
					matchesMap.put(table, matches);
					modifiedsMap.put(table, modified);
					missingsMap.put(table, missing);
				}
			}
		}

		if(!deleteRows.isEmpty()) {
			List<Column> pkColumns = primaryKey.getColumns();
			// Delete the rows in one big prepared statement, logging output
			StringBuilder deleteSql = new StringBuilder();
			deleteSql.append("DELETE FROM\n"
					+ "  \"").append(schema).append("\".\"").append(table.getName()).append("\"\n"
					+ "WHERE\n");
			boolean didOneRow = false;
			for(Row deleteRow : deleteRows) {
				if(didOneRow) deleteSql.append("  OR (");
				else {
					deleteSql.append("  (");
					didOneRow = true;
				}
				boolean didOneColumn = false;
				for(Column pkColumn : pkColumns) {
					if(didOneColumn) deleteSql.append(" AND ");
					else didOneColumn = true;
					deleteSql.append('"').append(pkColumn.getName()).append("\"=?");
				}
				deleteSql.append(")\n");
				stepOutput.append(
					ApplicationResources.accessor.getMessage(
						"JdbcResourceSynchronizer.deleteExtraRows.delete",
						schema,
						table,
						deleteRow.getPrimaryKeyValues()
					)
				).append('\n');
			}
			try (PreparedStatement pstmt = toConn.prepareStatement(deleteSql.toString())) {
				int pos = 1;
				for(Row deleteRow : deleteRows) {
					for(Column pkColumn : pkColumns) {
						pstmt.setObject(
							pos++,
							deleteRow.values[pkColumn.getOrdinalPosition()-1]
						);
					}
				}
				int numDeleted = pstmt.executeUpdate();
				if(numDeleted!=deleteRows.size()) throw new SQLException("Unexpected number of rows deleted for "+schema+"."+table.getName()+": Expected "+deleteRows.size()+", got "+numDeleted);
			}
		}
		deletesMap.put(table, (long)deleteRows.size());
	}

	/**
	 * Gets the real value for a row and column, using the propery byte[] instead of md5 hash.
	 */
	private static Object getRealValue(Connection fromConn, Row row, Column column) throws SQLException {
		switch(column.getDataType()) {
			// These were verified using md5, lookup the actual value
			case Types.BINARY :
			case Types.BLOB :
			case Types.LONGVARBINARY :
			case Types.VARBINARY :
			{
				Table table = column.getTable();
				StringBuilder sql = new StringBuilder();
				sql.append("SELECT \"").append(column.getName()).append("\" FROM \"").append(table.getSchema().getName()).append("\".\"").append(table.getName()).append("\" WHERE ");
				boolean didOne = false;
				for(Column pkColumn : table.getPrimaryKey().getColumns()) {
					if(didOne) sql.append(" AND ");
					else didOne = true;
					sql.append('"').append(pkColumn.getName()).append("\"=?");
				}
				try (PreparedStatement pstmt = fromConn.prepareStatement(sql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
					int pos = 1;
					for(Column pkColumn : table.getPrimaryKey().getColumns()) {
						pstmt.setObject(
							pos++,
							row.values[pkColumn.getOrdinalPosition()-1]
						);
					}
					try (ResultSet results = pstmt.executeQuery()) {
						if(!results.next()) throw new NoRowException();
						Object realValue = results.getObject(1);
						if(results.next()) throw new SQLException("More than one row returned");
						return realValue;
					}
				}
			}
			// All others are already fully loaded
			default :
				return row.values[column.getOrdinalPosition()-1];
		}
	}

	/**
	 * Updates and inserts rows.
	 */
	private static void updateAndInsertRows(
		Connection fromConn,
		Connection toConn,
		int synchronizeTimeout,
		Table table,
		StringBuilder stepOutput,
		Map<Table, Long> matchesMap,
		Map<Table, Long> modifiedsMap,
		Map<Table, Long> missingsMap,
		Map<Table, Long> updatesMap,
		Map<Table, Long> insertsMap
	) throws SQLException {
		final String schema = table.getSchema().getName();
		final List<Column> columns = table.getColumns();
		final Index primaryKey = table.getPrimaryKey();
		// Find rows to update and insert
		List<Row> updateRows = new ArrayList<>();
		List<Row> insertRows = new ArrayList<>();
		String selectSql = getSelectSql(table);
		try (Statement fromStmt = fromConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
			fromStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
			fromStmt.setFetchSize(FETCH_SIZE);
			try (Statement toStmt = toConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
				toStmt.setFetchDirection(ResultSet.FETCH_FORWARD);
				toStmt.setFetchSize(FETCH_SIZE);
				try (
					ResultSet fromResults = fromStmt.executeQuery(selectSql);
					ResultSet toResults = toStmt.executeQuery(selectSql)
				) {
					long matches = 0;
					RowIterator fromIter = new RowIterator(columns, primaryKey, fromResults);
					RowIterator toIter = new RowIterator(columns, primaryKey, toResults);
					while(true) {
						Row fromRow = fromIter.peek();
						Row toRow = toIter.peek();
						if(fromRow!=null) {
							if(toRow!=null) {
								int primaryKeyDiff = fromRow.compareTo(toRow);
								if(primaryKeyDiff==0) {
									// Primary keys have already been compared and are known to be equal, only need to compare the remaining columns
									if(fromRow.equalsNonPrimaryKey(toRow)) {
										// Exact match, remove both
										matches++;
									} else {
										updateRows.add(fromRow);
									}
									fromIter.remove();
									toIter.remove();
								} else if(primaryKeyDiff<0) {
									// Missing
									insertRows.add(fromRow);
									fromIter.remove();
								} else {
									assert primaryKeyDiff>0;
									// Extra
									throw new SQLException("Should already have been deleted from "+schema+"."+table.getName()+": "+toRow.getPrimaryKeyValues());
									// toIter.remove();
								}
							} else {
								// Missing
								insertRows.add(fromRow);
								fromIter.remove();
							}
						} else {
							if(toRow!=null) {
								// Extra
								throw new SQLException("Should already have been deleted from "+schema+"."+table.getName()+": "+toRow.getPrimaryKeyValues());
								//toIter.remove();
							} else {
								// All rows done
								break;
							}
						}
					}
					if(matches!=matchesMap.get(table)) throw new SQLException("Unexpected number of matches on second pass of "+schema+"."+table.getName()+": Expected "+matchesMap.get(table)+", got "+matches);
					if(updateRows.size()!=modifiedsMap.get(table)) throw new SQLException("Unexpected number of modifieds on second pass of "+schema+"."+table.getName()+": Expected "+modifiedsMap.get(table)+", got "+updateRows.size());
					if(insertRows.size()!=missingsMap.get(table)) throw new SQLException("Unexpected number of missings on second pass of "+schema+"."+table.getName()+": Expected "+missingsMap.get(table)+", got "+insertRows.size());
				}
			}
		}

		List<Column> pkColumns = primaryKey.getColumns();
		if(!updateRows.isEmpty()) {
			// Updates the rows in a batched prepared statement, logging output
			StringBuilder updateSql = new StringBuilder();
			updateSql.append("UPDATE\n"
					+ "  \"").append(schema).append("\".\"").append(table.getName()).append("\"\n"
					+ "SET");
			boolean didOneColumn = false;
			for(Column column : getNonPrimaryKeyColumns(columns, pkColumns)) {
				if(didOneColumn) updateSql.append(",\n  \"");
				else {
					updateSql.append("\n  \"");
					didOneColumn = true;
				}
				updateSql.append(column.getName()).append("\"=?");
			}
			updateSql.append("\n"
					+ "WHERE\n");
			didOneColumn = false;
			for(Column pkColumn : pkColumns) {
				if(didOneColumn) updateSql.append("  AND ");
				else {
					updateSql.append("  ");
					didOneColumn = true;
				}
				updateSql.append('"').append(pkColumn.getName()).append("\"=?\n");
			}
			try (PreparedStatement pstmt = toConn.prepareStatement(updateSql.toString())) {
				for(Row updateRow : updateRows) {
					stepOutput.append(
						ApplicationResources.accessor.getMessage(
							"JdbcResourceSynchronizer.updateAndInsertRows.update",
							schema,
							table,
							updateRow.getPrimaryKeyValues()
						)
					).append('\n');
					int pos = 1;
					for(Column column : getNonPrimaryKeyColumns(columns, pkColumns)) {
						pstmt.setObject(
							pos++,
							getRealValue(fromConn, updateRow, column)
						);
					}
					for(Column pkColumn : pkColumns) {
						pstmt.setObject(
							pos++,
							updateRow.values[pkColumn.getOrdinalPosition()-1]
						);
					}
					pstmt.addBatch();
				}
				int[] counts = pstmt.executeBatch();
				if(counts.length!=updateRows.size()) throw new SQLException("Unexpected batch size for "+schema+"."+table.getName()+": Expected "+updateRows.size()+", got "+counts.length);
				for(int c=0;c<counts.length;c++) {
					if(counts[c]!=1) throw new SQLException("Unexpected update count for "+schema+"."+table.getName()+": Expected 1, got "+counts[c]);
				}
			}
		}
		updatesMap.put(table, (long)updateRows.size());

		if(!insertRows.isEmpty()) {
			// Updates the rows in a batched prepared statement, logging output
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO\n"
					+ "  \"").append(schema).append("\".\"").append(table.getName()).append("\"\n"
					+ "(");
			boolean didOneColumn = false;
			for(Column column : columns) {
				if(didOneColumn) insertSql.append(",\n  \"");
				else {
					insertSql.append("\n  \"");
					didOneColumn = true;
				}
				insertSql.append(column.getName()).append('"');
			}
			insertSql.append("\n"
					+ ") VALUES (");
			didOneColumn = false;
			for(Column column : columns) {
				if(didOneColumn) insertSql.append(",\n  ?");
				else {
					insertSql.append("\n  ?");
					didOneColumn = true;
				}
			}
			insertSql.append("\n"
					+ ")");
			try (PreparedStatement pstmt = toConn.prepareStatement(insertSql.toString())) {
				for(Row insertRow : insertRows) {
					stepOutput.append(
						ApplicationResources.accessor.getMessage(
							"JdbcResourceSynchronizer.updateAndInsertRows.insert",
							schema,
							table,
							insertRow.getPrimaryKeyValues()
						)
					).append('\n');
					int pos = 1;
					for(Column column : columns) {
						pstmt.setObject(
							pos++,
							getRealValue(fromConn, insertRow, column)
						);
					}
					pstmt.addBatch();
				}
				int[] counts = pstmt.executeBatch();
				if(counts.length!=insertRows.size()) throw new SQLException("Unexpected batch size for "+schema+"."+table.getName()+": Expected "+insertRows.size()+", got "+counts.length);
				for(int c=0;c<counts.length;c++) {
					if(counts[c]!=1) throw new SQLException("Unexpected insert count for "+schema+"."+table.getName()+": Expected 1, got "+counts[c]);
				}
			}
		}
		insertsMap.put(table, (long)insertRows.size());
	}
}
