## v1.1.1 - Released
This release contains optimisation of memory utilization for large files writing to S3-storage.

### Bug fixes
* [FOLS3CL-18](https://issues.folio.org/browse/FOLS3CL-18) - folio-s3-client OOM during input stream writing


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