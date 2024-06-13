//  Copyright (c) 2024-present, Nicolae Mihalache.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * YamcsParchiveMergeOperator is a merge operator that merges Yamcs parameter archive segments.
 */
public class YamcsParchiveMergeOperator extends MergeOperator {
  public YamcsParchiveMergeOperator() {
    super(newYamcsParchiveMergeOperator());
  }

  private static native long newYamcsParchiveMergeOperator();

  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }

  private static native void disposeInternalJni(final long handle);
}
