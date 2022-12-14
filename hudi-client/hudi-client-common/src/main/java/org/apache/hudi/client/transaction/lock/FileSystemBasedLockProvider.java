/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.transaction.lock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.lock.LockProvider;
import org.apache.hudi.common.lock.LockState;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieLockException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_EXPIRE_PROP_KEY;
import static org.apache.hudi.common.config.LockConfiguration.FILESYSTEM_LOCK_PATH_PROP_KEY;

/**
 * A FileSystem based lock. This {@link LockProvider} implementation allows to lock table operations
 * using DFS. Users might need to manually clean the Locker's path if writeClient crash and never run again.
 * NOTE: This only works for DFS with atomic create/delete operation
 */
public class FileSystemBasedLockProvider implements LockProvider<String>, Serializable {

  private static final Logger LOG = LogManager.getLogger(FileSystemBasedLockProvider.class);

  private static final String LOCK_FILE_NAME = "lock";

  private final int lockTimeoutMinutes;
  private final transient FileSystem fs;
  private final transient Path lockFile;
  protected LockConfiguration lockConfiguration;
  private String lockInfo;
  private HoodieInstant ownerInstant;

  public FileSystemBasedLockProvider(final LockConfiguration lockConfiguration, final Configuration configuration) {
    checkRequiredProps(lockConfiguration);
    this.lockConfiguration = lockConfiguration;
    String lockDirectory = lockConfiguration.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY, null);
    if (StringUtils.isNullOrEmpty(lockDirectory)) {
      lockDirectory = lockConfiguration.getConfig().getString(HoodieWriteConfig.BASE_PATH.key())
            + Path.SEPARATOR + HoodieTableMetaClient.METAFOLDER_NAME;
    }
    this.lockTimeoutMinutes = lockConfiguration.getConfig().getInteger(FILESYSTEM_LOCK_EXPIRE_PROP_KEY);
    this.lockFile = new Path(lockDirectory + Path.SEPARATOR + LOCK_FILE_NAME);
    this.fs = FSUtils.getFs(this.lockFile.toString(), configuration);
  }

  @Override
  public void close() {
    synchronized (LOCK_FILE_NAME) {
      try {
        fs.delete(this.lockFile, true);
      } catch (IOException e) {
        throw new HoodieLockException(generateLogStatement(LockState.FAILED_TO_RELEASE), e);
      }
    }
  }

  @Override
  public boolean tryLock(Option<HoodieInstant> ownerInstant, long time, TimeUnit unit) {
    try {
      synchronized (LOCK_FILE_NAME) {
        // Check whether lock is already expired, if so try to delete lock file
        if (fs.exists(this.lockFile)) {
          if (checkIfExpired()) {
            fs.delete(this.lockFile, true);
            LOG.warn("Delete expired lock file: " + this.lockFile);
          } else {
            setLockInfo(readLockInfo());
            return false;
          }
        }
        if (ownerInstant.isPresent()) {
          this.ownerInstant = ownerInstant.get();
        }
        acquireLock();
        return fs.exists(this.lockFile);
      }
    } catch (IOException | HoodieIOException e) {
      LOG.info(generateLogStatement(LockState.FAILED_TO_ACQUIRE), e);
      return false;
    }
  }

  @Override
  public void unlock() {
    synchronized (LOCK_FILE_NAME) {
      try {
        if (fs.exists(this.lockFile)) {
          fs.delete(this.lockFile, true);
        }
      } catch (IOException io) {
        throw new HoodieIOException(generateLogStatement(LockState.FAILED_TO_RELEASE), io);
      }
    }
  }

  @Override
  public String getLock() {
    return this.lockFile.toString();
  }

  @Override
  public String getLockInfo() {
    return lockInfo;
  }

  public void setLockInfo(String lockInfo) {
    this.lockInfo = lockInfo;
  }

  private boolean checkIfExpired() {
    if (lockTimeoutMinutes == 0) {
      return false;
    }
    try {
      long modificationTime = fs.getFileStatus(this.lockFile).getModificationTime();
      if (System.currentTimeMillis() - modificationTime > lockTimeoutMinutes * 60 * 1000L) {
        return true;
      }
    } catch (IOException | HoodieIOException e) {
      LOG.error(generateLogStatement(LockState.ALREADY_RELEASED) + " failed to get lockFile's modification time", e);
    }
    return false;
  }

  private void acquireLock() {
    try {
      if (!fs.exists(this.lockFile)) {
        FSDataOutputStream fos = fs.create(this.lockFile, false);
        setLockInfo(generateLockInfo());
        fos.writeBytes(lockInfo);
        fos.close();
      }
    } catch (IOException e) {
      throw new HoodieIOException(generateLogStatement(LockState.FAILED_TO_ACQUIRE), e);
    }
  }

  public String generateLockInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("LOCK-TIME : %s \n", System.currentTimeMillis()));
    sb.append(String.format("LOCK-THREAD : %s \n", Thread.currentThread().getName()));
    if (this.ownerInstant != null) {
      sb.append(String.format("OWNER-INSTANT : %s \n", this.ownerInstant));
    }
    sb.append("LOCK-STACK-INFO : \n");
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    for (StackTraceElement ste : stack) {
      sb.append(String.format("\t %s.%s (%s:%s) \n", ste.getClassName(), ste.getMethodName(),
              ste.getFileName(), ste.getLineNumber()));
    }
    return sb.toString();
  }

  public String readLockInfo() {
    try {
      if (fs.exists(this.lockFile)) {
        FSDataInputStream fis = fs.open(this.lockFile);
        return FileIOUtils.readAsUTFString(fis);
      } else {
        return "";
      }
    } catch (IOException e) {
      throw new HoodieIOException(generateLogStatement(LockState.FAILED_TO_ACQUIRE), e);
    }
  }

  protected String generateLogStatement(LockState state) {
    return StringUtils.join(state.name(), " lock at: ", getLock());
  }

  private void checkRequiredProps(final LockConfiguration config) {
    ValidationUtils.checkArgument(config.getConfig().getString(FILESYSTEM_LOCK_PATH_PROP_KEY, null) != null
          || config.getConfig().getString(HoodieWriteConfig.BASE_PATH.key(), null) != null);
    ValidationUtils.checkArgument(config.getConfig().getInteger(FILESYSTEM_LOCK_EXPIRE_PROP_KEY) >= 0);
  }
}
