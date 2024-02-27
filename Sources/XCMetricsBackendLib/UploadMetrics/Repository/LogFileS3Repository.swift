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

import Foundation
import Vapor
import ClientRuntime
import AWSS3

/// `LogFileRepository` that uses Amazon S3 to store and fetch logs
struct LogFileS3Repository: LogFileRepository {

    let bucketName: String
    let group: EventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
    let s3Client: S3Client

    
    init?(bucketName: String, regionName: String) {
        guard let client = try? S3Client(region: regionName) else {
            print("Failed to initialize S3 client")
            return nil
        }
        self.s3Client = client
        self.bucketName = bucketName
    }

    init?(config: Configuration) {
        guard let bucketName = config.s3Bucket, 
              let regionName = config.s3Region else {
            return nil
        }
        self.init(bucketName: bucketName, regionName: regionName)
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
            let _ = try await self.s3Client.putObject(input: input)
            promise.succeed(())
        }
        try promise.futureResult.wait()
        
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
        
        let input = GetObjectInput(
            bucket: bucket,
            key: fileName
        )
        
        let promise = group.next().makePromise(of: Data.self)
        Task {
            let output = try await self.s3Client.getObject(input: input)
            
            // Get the data stream object. Return immediately if there isn't one.
            guard let body = output.body,
                  let data = try await body.readData() else {
                promise.fail(RepositoryError.unexpected(message: "There was an error downloading file \(logURL)"))
                return
            }
            promise.succeed(data)
        }
        let data = try promise.futureResult.wait()
    
        let tmp = try TemporaryFile(creatingTempDirectoryForFilename: "\(UUID().uuidString).xcactivitylog")
        try data.write(to: tmp.fileURL)
        return LogFile(remoteURL: logURL, localURL: tmp.fileURL)
    }
}