// Copyright (c) 2020 Spotify AB.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import Fluent
import FluentPostgresDriver
import QueuesRedisDriver
import Redis
import Vapor

// configures your application
public func configure(_ app: Application) async throws {

    let config = Configuration()
    // Enable gzip support
    app.http.server.configuration.requestDecompression = .enabled(limit: .none)

    // Database configuration
    if config.useCloudSQLSocket {
        let dbSocketDir = "/cloudsql"
        guard let cloudSQLInstanceConnectionName = config.cloudSQLConnectionName else {
            app.logger.error("XCMETRICS_USE_CLOUDSQL_SOCKET is specified but XCMETRICS_CLOUDSQL_CONNECTION_NAME not found")
            preconditionFailure()
        }
        let socketPath = "\(dbSocketDir)/\(cloudSQLInstanceConnectionName)/.s.PGSQL.5432"
        app.logger.notice("Connecting to \(config.databaseName) in \(socketPath) as \(config.databaseUser) password length \(config.databasePassword.count)")
        let postgresConfig = PostgresConfiguration(unixDomainSocketPath: socketPath,
                                                   username: config.databaseUser,
                                                   password: config.databasePassword,
                                                   database: nil)

        app.databases.use(.postgres(configuration: postgresConfig, maxConnectionsPerEventLoop: 10), as: .psql)
    } else {

        app.logger.notice("Connecting to \(config.databaseName) in \(config.databaseHost) as \(config.databaseUser) password length \(config.databasePassword.count)")

        app.databases.use(.postgres(
            hostname: config.databaseHost,
            port: config.databasePort,
            username: config.databaseUser,
            password: config.databasePassword,
            database: config.databaseName,
            maxConnectionsPerEventLoop: 10
        ), as: .psql)
    }

    // Add database migrations
    app.migrations.add(CreateBuild(),
                       CreateBuildMetadata(),
                       CreateTarget(),
                       CreateStep(),
                       CreateBuildWarning(),
                       CreateBuildError(),
                       CreateBuildNotes(),
                       CreateSwiftFunction(),
                       CreateSwiftTypeCheck(),
                       CreateBuildHosts(),
                       CreateXcodeVersion(),
                       JobLogEntryMigration(),
                       AddDetailsToErrors(),
                       AddDetailsToNotes(),
                       AddDetailsToWarnings(),
                       AddBuildStatusIndex(),
                       AddProjectNameIndex(),
                       AddBuildIdentifierIndexToTarget(),
                       AddBuildIdentifierIndexToBuildErrors(),
                       AddBuildIdentifierIndexToStep(),
                       AddBuildIdentifierIndexToBuildWarnings(),
                       AddBuildIdentifierIndexToBuildNotes(),
                       AddBuildIdentifierIndexToBuildHost(),
                       AddBuildIdentifierIndexToSwiftFunctions(),
                       AddBuildIdentifierIndexToSwiftTypeChecks(),
                       AddBuildIdentifierIndexToXcodeVersion(),
                       AddBuildIdentifierIndexToBuildMetadata(),
                       AddTargetIdentifierIndexToSteps(),
                       AddStepIdentifierIndexToSwiftFunctions(),
                       AddStepIdentifierIndexToSwiftTypeChecks(),
                       CreateDayCount(),
                       CreateDayBuildTime()
                       )


    if config.useAsyncLogProcessing && app.environment != .testing {
        app.logger.info("Using redis host \(config.redisHost) and port \(config.redisPort)")
        app.queues.add(JobLogEventDelegate(logger: app.logger,
                                           repository: PostgreSQLJobLogRepository(db: app.db)))
        let redisConfig = try RedisConfiguration(
            hostname: config.redisHost,
            port: config.redisPort,
            password: config.redisPassword,
            pool: RedisConfiguration.PoolOptions(maximumConnectionCount: .maximumActiveConnections(2),
                                                 minimumConnectionCount: 0,
                                                 connectionBackoffFactor: 2,
                                                 initialConnectionBackoffDelay: .milliseconds(100),
                                                 connectionRetryTimeout: config.redisConnectionTimeout))
        app.queues.use(.redis(redisConfig))
    } else {
        app.logger.info("Async log processing is disabled")
    }

    // Scheduled jobs
    if config.scheduleStatisticsJobs && app.environment != .testing {
        app.queues
            .schedule(DailyStatisticsJob(repository: SQLStatisticsRepository(db: app.db)))
            .daily()
            .at(.midnight)
    }

    // register routes
    try await routes(app)
}

