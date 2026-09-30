// Copyright (c) 2026 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// clang-format off
//go:build driverlib
// clang-format on

#pragma once

#include "adbc.h"
#include <stdlib.h>

struct AdbcError* RedisErrorFromArrayStream(struct ArrowArrayStream*, AdbcStatusCode*);
AdbcStatusCode RedisDatabaseGetOption(struct AdbcDatabase*, const char*, char*, size_t*, struct AdbcError*);
AdbcStatusCode RedisDatabaseGetOptionBytes(struct AdbcDatabase*, const char*, uint8_t*, size_t*, struct AdbcError*);
AdbcStatusCode RedisDatabaseGetOptionDouble(struct AdbcDatabase*, const char*, double*, struct AdbcError*);
AdbcStatusCode RedisDatabaseGetOptionInt(struct AdbcDatabase*, const char*, int64_t*, struct AdbcError*);
AdbcStatusCode RedisDatabaseInit(struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode RedisDatabaseNew(struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode RedisDatabaseRelease(struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode RedisDatabaseSetOption(struct AdbcDatabase* db, const char* key, const char* value, struct AdbcError* err);
AdbcStatusCode RedisDatabaseSetOptionBytes(struct AdbcDatabase*, const char*, const uint8_t*, size_t, struct AdbcError*);
AdbcStatusCode RedisDatabaseSetOptionDouble(struct AdbcDatabase*, const char*, double, struct AdbcError*);
AdbcStatusCode RedisDatabaseSetOptionInt(struct AdbcDatabase*, const char*, int64_t, struct AdbcError*);

AdbcStatusCode RedisConnectionCancel(struct AdbcConnection*, struct AdbcError*);
AdbcStatusCode RedisConnectionCommit(struct AdbcConnection* cnxn, struct AdbcError* err);
AdbcStatusCode RedisConnectionGetInfo(struct AdbcConnection* cnxn, const uint32_t* codes, size_t len, struct ArrowArrayStream* out, struct AdbcError* err);
AdbcStatusCode RedisConnectionGetObjects(struct AdbcConnection* cnxn, int depth, const char* catalog, const char* dbSchema, const char* tableName, const char** tableType, const char* columnName, struct ArrowArrayStream* out, struct AdbcError* err);
AdbcStatusCode RedisConnectionGetOption(struct AdbcConnection*, const char*, char*, size_t*, struct AdbcError*);
AdbcStatusCode RedisConnectionGetOptionBytes(struct AdbcConnection*, const char*, uint8_t*, size_t*, struct AdbcError*);
AdbcStatusCode RedisConnectionGetOptionDouble(struct AdbcConnection*, const char*, double*, struct AdbcError*);
AdbcStatusCode RedisConnectionGetOptionInt(struct AdbcConnection*, const char*, int64_t*, struct AdbcError*);
AdbcStatusCode RedisConnectionGetStatistics(struct AdbcConnection*, const char*, const char*, const char*, char, struct ArrowArrayStream*, struct AdbcError*);
AdbcStatusCode RedisConnectionGetStatisticNames(struct AdbcConnection*, struct ArrowArrayStream*, struct AdbcError*);
AdbcStatusCode RedisConnectionGetTableSchema(struct AdbcConnection* cnxn, const char* catalog, const char* dbSchema, const char* tableName, struct ArrowSchema* schema, struct AdbcError* err);
AdbcStatusCode RedisConnectionGetTableTypes(struct AdbcConnection* cnxn, struct ArrowArrayStream* out, struct AdbcError* err);
AdbcStatusCode RedisConnectionInit(struct AdbcConnection* cnxn, struct AdbcDatabase* db, struct AdbcError* err);
AdbcStatusCode RedisConnectionNew(struct AdbcConnection* cnxn, struct AdbcError* err);
AdbcStatusCode RedisConnectionReadPartition(struct AdbcConnection* cnxn, const uint8_t* serialized, size_t serializedLen, struct ArrowArrayStream* out, struct AdbcError* err);
AdbcStatusCode RedisConnectionRelease(struct AdbcConnection* cnxn, struct AdbcError* err);
AdbcStatusCode RedisConnectionRollback(struct AdbcConnection* cnxn, struct AdbcError* err);
AdbcStatusCode RedisConnectionSetOption(struct AdbcConnection* cnxn, const char* key, const char* val, struct AdbcError* err);
AdbcStatusCode RedisConnectionSetOptionBytes(struct AdbcConnection*, const char*, const uint8_t*, size_t, struct AdbcError*);
AdbcStatusCode RedisConnectionSetOptionDouble(struct AdbcConnection*, const char*, double, struct AdbcError*);
AdbcStatusCode RedisConnectionSetOptionInt(struct AdbcConnection*, const char*, int64_t, struct AdbcError*);

AdbcStatusCode RedisStatementBind(struct AdbcStatement* stmt, struct ArrowArray* values, struct ArrowSchema* schema, struct AdbcError* err);
AdbcStatusCode RedisStatementBindStream(struct AdbcStatement* stmt, struct ArrowArrayStream* stream, struct AdbcError* err);
AdbcStatusCode RedisStatementCancel(struct AdbcStatement*, struct AdbcError*);
AdbcStatusCode RedisStatementExecuteQuery(struct AdbcStatement* stmt, struct ArrowArrayStream* out, int64_t* affected, struct AdbcError* err);
AdbcStatusCode RedisStatementExecutePartitions(struct AdbcStatement* stmt, struct ArrowSchema* schema, struct AdbcPartitions* partitions, int64_t* affected, struct AdbcError* err);
AdbcStatusCode RedisStatementExecutePartitionsTrampoline(struct AdbcStatement* stmt, struct ArrowSchema* schema, struct AdbcPartitions* partitions, int64_t* affected, struct AdbcError* err);
AdbcStatusCode RedisStatementExecuteSchema(struct AdbcStatement*, struct ArrowSchema*, struct AdbcError*);
AdbcStatusCode RedisStatementGetOption(struct AdbcStatement*, const char*, char*, size_t*, struct AdbcError*);
AdbcStatusCode RedisStatementGetOptionBytes(struct AdbcStatement*, const char*, uint8_t*, size_t*, struct AdbcError*);
AdbcStatusCode RedisStatementGetOptionDouble(struct AdbcStatement*, const char*, double*, struct AdbcError*);
AdbcStatusCode RedisStatementGetOptionInt(struct AdbcStatement*, const char*, int64_t*, struct AdbcError*);
AdbcStatusCode RedisStatementGetParameterSchema(struct AdbcStatement* stmt, struct ArrowSchema* schema, struct AdbcError* err);
AdbcStatusCode RedisStatementNew(struct AdbcConnection* cnxn, struct AdbcStatement* stmt, struct AdbcError* err);
AdbcStatusCode RedisStatementPrepare(struct AdbcStatement* stmt, struct AdbcError* err);
AdbcStatusCode RedisStatementRelease(struct AdbcStatement* stmt, struct AdbcError* err);
AdbcStatusCode RedisStatementSetOption(struct AdbcStatement* stmt, const char* key, const char* value, struct AdbcError* err);
AdbcStatusCode RedisStatementSetOptionBytes(struct AdbcStatement*, const char*, const uint8_t*, size_t, struct AdbcError*);
AdbcStatusCode RedisStatementSetOptionDouble(struct AdbcStatement*, const char*, double, struct AdbcError*);
AdbcStatusCode RedisStatementSetOptionInt(struct AdbcStatement*, const char*, int64_t, struct AdbcError*);
AdbcStatusCode RedisStatementSetSqlQuery(struct AdbcStatement* stmt, const char* query, struct AdbcError* err);
AdbcStatusCode RedisStatementSetSubstraitPlan(struct AdbcStatement* stmt, const uint8_t* plan, size_t length, struct AdbcError* err);

AdbcStatusCode AdbcDriverRedisInit(int version, void* rawDriver, struct AdbcError* err);

static inline void RediserrRelease(struct AdbcError* error) {
  if (error->release) {
    error->release(error);
    error->release = NULL;
  }
}

void Redis_release_error(struct AdbcError* error);

struct RedisError {
  char* message;
  char** keys;
  uint8_t** values;
  size_t* lengths;
  int count;
};

void RedisReleaseErrWithDetails(struct AdbcError* error);

int RedisErrorGetDetailCount(const struct AdbcError* error);
struct AdbcErrorDetail RedisErrorGetDetail(const struct AdbcError* error, int index);

int RedisArrayStreamGetSchemaTrampoline(struct ArrowArrayStream* stream, struct ArrowSchema* out);
int RedisArrayStreamGetNextTrampoline(struct ArrowArrayStream* stream, struct ArrowArray* out);
