## v2.0.7 - Released
This release contains security vulnerability fixes in dependencies.

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.0.6...v2.0.7)

### Bug fixes
* [FOLS3CL-26](https://issues.folio.org/browse/FOLS3CL-26) minio 8.5.7 fixing security vulnerabilities

## v2.0.6 - Released
This release contains fixing the vulnerabilities of AWS SDK

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.0.5...v2.0.6)

### Bug fixes
* [FOLS3CL-23](https://issues.folio.org/browse/FOLS3CL-23) software.amazon.awssdk:s3 security vulnerabilities

## v2.0.4 - Released
This release contains improving of folio-s3-client read-method

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.0.3...v2.0.4)

## v2.0.3 - Released
This release contains reverting of AWS SDK version and improvements

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.0.2...v2.0.3)

### Tasks
* [FOLS3CL-20](https://issues.folio.org/browse/FOLS3CL-20) Java 17, upgrade dependencies for Poppy

## v2.0.2 - Released
This release contains improving of folio-s3-client read-method

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.0.1...v2.0.2)

### Tasks
* [FOLS3CL-18](https://issues.folio.org/browse/FOLS3CL-18) folio-s3-client OOM during input stream writing

## v2.0.1 - Released
This release contains reverting of AWS SDK version

[Full Changelog](https://github.com/folio-org/folio-s3-client/compare/v2.0.0...v2.0.1)

### Tasks
* [FOLS3CL-20](https://issues.folio.org/browse/FOLS3CL-20) Java 17, upgrade dependencies for Poppy

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
