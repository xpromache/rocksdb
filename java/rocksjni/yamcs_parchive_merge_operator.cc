//  Copyright (c) 2024-present, Nicolae Mihalache.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include "include/org_rocksdb_YamcsParchiveMergeOperator.h"
#include "utilities/yamcs/merge_operator.h"
#include "rocksjni/cplusplus_to_java_convert.h"

/*
 * Class:     org_rocksdb_YamcsParchiveMergeOperator
 * Method:    newYamcsParchiveMergeOperator
 * Signature: (II)J
 */
jlong Java_org_rocksdb_YamcsParchiveMergeOperator_newYamcsParchiveMergeOperator(
    JNIEnv* /*env*/, jclass /*jclazz*/) {
  auto* op = new std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>(
      new ROCKSDB_NAMESPACE::yamcs::YamcsParchiveMergeOperator());
  return GET_CPLUSPLUS_POINTER(op);
}

/*
 * Class:     org_rocksdb_YamcsParchiveMergeOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_YamcsParchiveMergeOperator_disposeInternalJni(
    JNIEnv* /*env*/, jclass /*jcls*/, jlong jhandle) {
  auto* op =
      reinterpret_cast<std::shared_ptr<ROCKSDB_NAMESPACE::MergeOperator>*>(
          jhandle);
  delete op;
}
