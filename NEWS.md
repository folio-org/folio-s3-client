## v2.3.0 - Released
This release contains upgrade to Java 21 and dependencies upgrading

### Stories
* [FOLS3CL-38](https://issues.folio.org/browse/FOLS3CL-38) Update to folio-s3-client Java 21

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.2.1...v2.3.0)

## v2.2.1 - Released
This release contains upgrade minio and aws sdk

### Stories
* [FOLS3CL-37](https://issues.folio.org/browse/FOLS3CL-37) Upgrade minio and aws sdk for Ramsons

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.2.0...v2.2.1)

## v2.2.0 - Released
This release contains improvement client configuration

### Tasks
* [FOLS3CL-33](https://issues.folio.org/browse/FOLS3CL-33) Add subPath functionality to folio S3 client
* [FOLS3CL-32](https://issues.folio.org/browse/FOLS3CL-32) Unit tests improvements

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.1.1...v2.2.0)

## v2.1.1 - Released
This release contains dependencies updates and bug fixes

This release contains dependencies updates.

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.1.0...v2.1.1)

### Bug fixes
* [FOLS3CL-29](https://folio-org.atlassian.net/browse/FOLS3CL-29) Upgrade dependencies for Quesnelia (minio, commons-compress, …)
* [FOLS3CL-31](https://folio-org.atlassian.net/browse/FOLS3CL-21) AWS S3: writing limitation

## v2.1.0 - Released
This release contains dependencies updates and bug fixes

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.0.0...v2.1.0)

### Tasks
* [FOLS3CL-20](https://issues.folio.org/browse/FOLS3CL-20) Java 17, upgrade dependencies for Poppy
* [FOLS3CL-18](https://issues.folio.org/browse/FOLS3CL-18) folio-s3-client OOM during input stream writing

### Bug fixes
* [FOLS3CL-26](https://issues.folio.org/browse/FOLS3CL-26) minio 8.5.7 fixing security vulnerabilities
* [FOLS3CL-23](https://issues.folio.org/browse/FOLS3CL-23) software.amazon.awssdk:s3 security vulnerabilities

## v2.0.0 - Released
This release contains only upgrade to Java 17

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v1.1.0...v2.0.0)

### Tasks
* [FOLS3CL-20](https://issues.folio.org/browse/FOLS3CL-20) Java 17, upgrade dependencies for Poppy

## v1.1.0 - 2023-10-06
* [FOLS3CL-11](https://issues.folio.org/browse/FOLS3CL-11) Add method for additional presigned request generation
* [FOLS3CL-13](https://issues.folio.org/browse/FOLS3CL-13) Add methods for multipart uploads

## v1.0.0 - Released
The initial release of the folio-s3-client.

### Stories
* [FOLS3CL-2](https://issues.folio.org/browse/FOLS3CL-2) - folio-s3-client implementation

### Bug fixes
* [FOLS3CL-4](https://issues.folio.org/browse/FOLS3CL-4) - Add maven-compiler-plugin version, remove duplicate maven-release-plugin
* [FOLS3CL-3](https://issues.folio.org/browse/FOLS3CL-3) - Upgrade aws-sdk-java, minio, netty, jackson-databind fixing vulns
