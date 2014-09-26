/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import com.google.common.base.Preconditions;

/**
 * Information about a single remote NameNode
 */
public class RemoteNameNodeInfo {

  public static List<RemoteNameNodeInfo> getRemoteNameNodes(Configuration conf) throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    return getRemoteNameNodes(conf, nsId);
  }

  public static List<RemoteNameNodeInfo> getRemoteNameNodes(Configuration conf, String nsId)
      throws IOException {
    // there is only a single NN configured (and no federation) so we don't have any more NNs
    if (nsId == null) {
      return Collections.emptyList();
    }
    List<Configuration> otherNodes = HAUtil.getConfForOtherNodes(conf);
    List<RemoteNameNodeInfo> nns = new ArrayList<RemoteNameNodeInfo>();

    for (Configuration otherNode : otherNodes) {
      String otherNNId = HAUtil.getNameNodeId(otherNode, nsId);
      // don't do any validation here as in some cases, it can be overwritten later
      InetSocketAddress otherIpcAddr = NameNode.getServiceAddress(otherNode, true);


      final String scheme = DFSUtil.getHttpClientScheme(conf);
      URL otherHttpAddr = DFSUtil.getInfoServerWithDefaultHost(otherIpcAddr.getHostName(),
          otherNode, scheme).toURL();

      nns.add(new RemoteNameNodeInfo(otherNode, otherNNId, otherIpcAddr, otherHttpAddr));
    }
    return nns;
  }

  private final Configuration conf;
  private final String nnId;
  private InetSocketAddress ipcAddress;
  private final URL httpAddress;

  private RemoteNameNodeInfo(Configuration conf, String nnId, InetSocketAddress ipcAddress,
      URL httpAddress) {
    this.conf = conf;
    this.nnId = nnId;
    this.ipcAddress = ipcAddress;
    this.httpAddress = httpAddress;
  }

  public InetSocketAddress getIpcAddress() {
    return this.ipcAddress;
  }

  public String getNameNodeID() {
    return this.nnId;
  }

  public URL getHttpAddress() {
    return this.httpAddress;
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  public void setIpcAddress(InetSocketAddress ipc) {
    this.ipcAddress = ipc;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((httpAddress == null) ? 0 : httpAddress.hashCode());
    result = prime * result
        + ((ipcAddress == null) ? 0 : ipcAddress.hashCode());
    result = prime * result + ((nnId == null) ? 0 : nnId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    RemoteNameNodeInfo other = (RemoteNameNodeInfo) obj;

    if (httpAddress == null) {
      if (other.httpAddress != null) return false;
    } else if (!httpAddress.equals(other.httpAddress)) return false;
    if (ipcAddress == null) {
      if (other.ipcAddress != null) return false;
    } else if (!ipcAddress.equals(other.ipcAddress)) return false;
    if (nnId == null) {
      if (other.nnId != null) return false;
    } else if (!nnId.equals(other.nnId)) return false;
    return true;
  }

  @Override
  public String toString() {
    return "RemoteNameNodeInfo [nnId=" + nnId + ", ipcAddress=" + ipcAddress
        + ", httpAddress=" + httpAddress + "]";
  }
}