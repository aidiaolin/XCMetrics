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

import AWSClientRuntime
import AWSS3
import ClientRuntime
import Foundation
import Logging
import Vapor

/// `LogFileRepository` that uses Amazon S3 to store and fetch logs
struct LogFileS3Repository: LogFileRepository {
    let bucketName: String
    let group: EventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
    let s3Client: S3Client
    let logger: Logger

    init?(bucketName: String, regionName: String, logger: Logger) {
        var client: S3Client?
        if Environment.get("AWS_PROFILE") != nil {
            // Initialize S3 client with SSOCredentialsProvider
            client = try? S3Client(
                config: S3Client.S3ClientConfiguration(
                    region: regionName,
                    credentialsProvider: SSOCredentialsProvider()
                )
            )
        } else {
            client = try? S3Client(region: regionName)
        }

        guard let s3Client = client else {
            logger.error("[LogFileS3Repository] Failed to initialize S3 client")
            return nil
        }

        self.s3Client = s3Client
        self.bucketName = bucketName
        self.logger = logger

//         Verify the client
        //  let promise = group.next().makePromise(of: Void.self)
        //  Task {
        //      do {
        //          let response = try await s3Client.listBuckets(input: ListBucketsInput()) // This will list all available S3 buckets
        //          promise.succeed(())
        //          print("Available buckets: \(String(describing: response.buckets))")
        //      } catch {
        //          print("Unable to retrieve the list of buckets, error: \(error.localizedDescription)")
        //          promise.fail(error)
        //      }
        //  }
        //  do {
        //      try promise.futureResult.wait()
        //  } catch {
        //      print("Error waiting for promise: \(error)")
        //  }
    }

    init?(config: Configuration, logger: Logger) {
        guard let bucketName = config.s3Bucket,
              let regionName = config.s3Region
        else {
            return nil
        }
        self.init(bucketName: bucketName, regionName: regionName, logger: logger)
    }

    func put(logFile: File) throws -> URL {
        let data = Data(logFile.data.xcm_onlyFileData().readableBytesView)

        let dataStream = ByteStream.data(data)

        let input = PutObjectInput(
            body: dataStream,
            bucket: bucketName,
            key: logFile.filename
        )

        let promise = group.next().makePromise(of: Void.self)
        Task {
            do {
                let _ = try await self.s3Client.putObject(input: input)
                promise.succeed(())
                logger.info("[LogFileS3Repository] Log file: '\(logFile.filename)' uploaded successfully to S3 bucket: '\(bucketName)'")
            } catch {
                logger.error("[LogFileS3Repository] Error uploading log file: '\(logFile.filename)' to S3 bucket: '\(bucketName)', Error: \(error)")
                promise.fail(error)
            }
        }

        do {
            try promise.futureResult.wait()
        } catch {
            logger.error("[LogFileS3Repository] Error waiting for the completion of the file upload for log file: '\(logFile.filename)' to S3 bucket: '\(bucketName)', Error: \(error)")
        }

        guard let url = URL(string: "s3://\(bucketName)/\(logFile.filename)") else {
            throw RepositoryError.unexpected(message: "Invalid url of \(logFile.filename)")
        }

        return url
    }

    func get(logURL: URL) throws -> LogFile {
        guard let bucket = logURL.host else {
            throw RepositoryError.unexpected(message: "URL is not an S3 url \(logURL)")
        }
        let fileName = logURL.lastPathComponent

        logger.info("[LogFileS3Repository] get \(logURL), bucket: \(bucket), object: \(fileName)")
        let input = GetObjectInput(
            bucket: bucket,
            key: fileName
        )

        let promise = group.next().makePromise(of: Data.self)
        Task {
            do {
                let output = try await self.s3Client.getObject(input: input)

                // Get the data stream object. Return immediately if there isn't one.
                guard let body = output.body, let data = try await body.readData() else {
                    promise.fail(RepositoryError.unexpected(message: "There was an error fetching file \(logURL)"))
                    return
                }
                promise.succeed(data)
            } catch {
                logger.error("[LogFileS3Repository] Failed to fetch file: \(error)")
                promise.fail(error)
            }
        }

        let data = try promise.futureResult.wait()
        let tmp = try TemporaryFile(creatingTempDirectoryForFilename: "\(UUID().uuidString).xcactivitylog")
        try data.write(to: tmp.fileURL)
        return LogFile(remoteURL: logURL, localURL: tmp.fileURL)
    }
}
