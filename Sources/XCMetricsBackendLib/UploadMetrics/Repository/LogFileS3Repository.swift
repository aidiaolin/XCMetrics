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
import AWSS3
import AWSClientRuntime

/// `LogFileRepository` that uses Amazon S3 to store and fetch logs
struct LogFileS3Repository: LogFileRepository {

    let bucketName: String
    let s3Client: S3Client

    init(bucketName: String, region: String,
         accessKeyId: String? = nil,
         secretAccessKey: String? = nil,
         sessionToken: String? = nil) async throws {
        self.bucketName = bucketName

        if accessKeyId == nil || secretAccessKey == nil {
            self.s3Client = try S3Client(region: region)
        } else if let keyId = accessKeyId, let secretKey = secretAccessKey {
            // If both are not nil, use the given access key ID, secret access key, and session token
            // to create credentialsProvider and S3 client
            // to generate a static credentials provider suitable for use when
            // initializing an Amazon S3 client.
            let credentialsProvider = try AWSClientRuntime.StaticCredentialsProvider(
                AWSClientRuntime.Credentials(accessKey: keyId,
                                             secret: secretKey,
                                             sessionToken: sessionToken
                )
            )
            let s3Config = try await S3Client.S3ClientConfiguration(
                credentialsProvider: credentialsProvider,
                region: region
            )
            self.s3Client = S3Client(config: s3Config)
        }
    }

    init?(config: Configuration) async throws {
        guard 
            let bucketName = config.s3Bucket, 
            let region = config.s3Region 
        else {
            return nil
        }

        let accessKeyId = config.awsAccessKeyId
        let secretAccessKeyId = config.awsSecretAccessKey

        try await self.init(
            bucketName: bucketName, 
            region: region, 
            accessKeyId: accessKeyId ?? nil, 
            secretAccessKey: secretAccessKeyId ?? nil
        )
    }

    func put(logFile: File) throws -> URL {
        let data = Data(logFile.data.xcm_onlyFileData().readableBytesView)
        let bucket = bucketName
        let key = logFile.filename

        let dataStream = ByteStream.from(data: data)

        // Create the `PutObjectInput`
        let input = PutObjectInput(body: dataStream,
                                        bucket: bucket,
                                        key: key)

        // Put the object to S3
        _ = try await s3Client.putObject(input: input)
        
        guard let url = URL(string: "s3://\(bucketName)/\(key)") else {
            throw RepositoryError.unexpected(message: "Invalid url of \(key)")
        }

        return url
    }

    func get(logURL: URL) async throws -> LogFile {
        guard let bucket = logURL.host else {
            throw RepositoryError.unexpected(message: "URL is not an S3 url \(logURL)")
        }
        let fileName = logURL.lastPathComponent

        // Create the `GetObjectInput`
        let input = GetObjectInput(bucket: bucket, key: fileName)

        // Get the object from S3
        let response = try await s3Client.getObject(input: input)

        // Convert the byte stream to Data
        let data = try await response.body?.toBytes().toData()
        guard let data = data else {
            throw RepositoryError.unexpected(message: "There was an error downloading file \(logURL)")
        }

        let tmp = try TemporaryFile(creatingTempDirectoryForFilename: "\(UUID().uuidString).xcactivitylog")
        let fileURL = tmp.fileURL
        try data.write(to: fileURL)
        return LogFile(remoteURL: logURL, localURL: fileURL)
    }

}
