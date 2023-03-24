# folio-s3-client

Copyright (C) 2021-2022 The Open Library Foundation

This software is distributed under the terms of the Apache License, Version 2.0. See the file "LICENSE" for more information.

## Introduction

This is a shared library for FOLIO S3-client.

## Overview

FOLIO S3-client supports both Minio and AWS S3 storages. It can be used when module should operate with data on S3 storage.
The minio client is used by default. Any module can use the aws client by including this dependency:
```
    <dependency>
      <groupId>software.amazon.awssdk</groupId>
      <artifactId>aws-sdk-java</artifactId>
      <version>2.19.2</version>
    </dependency>
```
### Issue tracker

See project [FOLS3CL](https://issues.folio.org/browse/FOLS3CL)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker).

### Other documentation

Other [modules](https://dev.folio.org/source-code/#server-side) are described,
with further FOLIO Developer documentation at [dev.folio.org](https://dev.folio.org/)
