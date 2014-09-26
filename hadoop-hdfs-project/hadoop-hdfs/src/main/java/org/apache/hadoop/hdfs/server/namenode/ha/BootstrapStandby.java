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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.tools.DFSHAAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Tool which allows the standby node's storage directories to be bootstrapped
 * by copying the latest namespace snapshot from the active namenode. This is
 * used when first configuring an HA cluster.
 */
@InterfaceAudience.Private
public class BootstrapStandby implements Tool, Configurable {
  private static final Log LOG = LogFactory.getLog(BootstrapStandby.class);
  private String nsId;
  private String nnId;
  private List<RemoteNameNodeInfo> remoteNNs;

  private Collection<URI> dirsToFormat;
  private List<URI> editUrisToFormat;
  private List<URI> sharedEditsUris;
  private Configuration conf;
  
  private boolean force = false;
  private boolean interactive = true;
  private boolean skipSharedEditsCheck = false;

  // Exit/return codes.
  static final int ERR_CODE_FAILED_CONNECT = 2;
  static final int ERR_CODE_INVALID_VERSION = 3;
  // Skip 4 - was used in previous versions, but no longer returned.
  static final int ERR_CODE_ALREADY_FORMATTED = 5;
  static final int ERR_CODE_LOGS_UNAVAILABLE = 6; 

  @Override
  public int run(String[] args) throws Exception {
    parseArgs(args);
    parseConfAndFindOtherNN();
    NameNode.checkAllowFormat(conf);

    InetSocketAddress myAddr = NameNode.getAddress(conf);
    SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, myAddr.getHostName());

    return SecurityUtil.doAsLoginUserOrFatal(new PrivilegedAction<Integer>() {
      @Override
      public Integer run() {
        try {
          return doRun();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
  
  private void parseArgs(String[] args) {
    for (String arg : args) {
      if ("-force".equals(arg)) {
        force = true;
      } else if ("-nonInteractive".equals(arg)) {
        interactive = false;
      } else if ("-skipSharedEditsCheck".equals(arg)) {
        skipSharedEditsCheck = true;
      } else {
        printUsage();
        throw new HadoopIllegalArgumentException(
            "Illegal argument: " + arg);
      }
    }
  }

  private void printUsage() {
    System.err.println("Usage: " + this.getClass().getSimpleName() +
        " [-force] [-nonInteractive] [-skipSharedEditsCheck]");
  }

  private NamenodeProtocol createNNProtocolProxy(InetSocketAddress otherIpcAddr)
      throws IOException {
    return NameNodeProxies.createNonHAProxy(getConf(),
        otherIpcAddr, NamenodeProtocol.class,
        UserGroupInformation.getLoginUser(), true)
        .getProxy();
  }
  
  private int doRun() throws IOException {

    // find the active NN
    NamenodeProtocol proxy = null;
    NamespaceInfo nsInfo = null;
    RemoteNameNodeInfo proxyInfo = null;
    for (int i = 0; i < remoteNNs.size(); i++) {
      proxyInfo = remoteNNs.get(i);
      InetSocketAddress otherIpcAddress = proxyInfo.getIpcAddress();
      proxy = createNNProtocolProxy(otherIpcAddress);
      try {
        // get the namespace from any active NN. On a fresh cluster, this is the active. On a
        // running cluster, this works on any node.
        nsInfo = proxy.versionRequest();
        break;
      } catch (IOException ioe) {
        LOG.warn("Unable to fetch namespace information from remote NN at " + otherIpcAddress
            + ": " + ioe.getMessage());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Full exception trace", ioe);
        }
      }
    }

    if (nsInfo == null) {
      LOG.fatal(
          "Unable to fetch namespace information from any remote NN. Possible NameNodes: "
              + remoteNNs);
      return ERR_CODE_FAILED_CONNECT;
    }

    if (!checkLayoutVersion(nsInfo)) {
      LOG.fatal("Layout version on remote node (" + nsInfo.getLayoutVersion()
          + ") does not match " + "this node's layout version ("
          + HdfsConstants.NAMENODE_LAYOUT_VERSION + ")");
      return ERR_CODE_INVALID_VERSION;
    }

    
    System.out.println(
        "=====================================================\n" +
        "About to bootstrap Standby ID " + nnId + " from:\n" +
        "           Nameservice ID: " + nsId + "\n" +
        "        Other Namenode ID: " + proxyInfo.getNameNodeID() + "\n" +
        "  Other NN's HTTP address: " + proxyInfo.getHttpAddress() + "\n" +
        "  Other NN's IPC  address: " + proxyInfo.getIpcAddress() + "\n" +
        "             Namespace ID: " + nsInfo.getNamespaceID() + "\n" +
        "            Block pool ID: " + nsInfo.getBlockPoolID() + "\n" +
        "               Cluster ID: " + nsInfo.getClusterID() + "\n" +
        "           Layout version: " + nsInfo.getLayoutVersion() + "\n" +
        "=====================================================");

    long imageTxId = proxy.getMostRecentCheckpointTxId();
    long curTxId = proxy.getTransactionID();
    
    NNStorage storage = new NNStorage(conf, dirsToFormat, editUrisToFormat);
    
    // Check with the user before blowing away data.
    if (!Storage.confirmFormat(storage.dirIterable(null),
            force, interactive)) {
      storage.close();
      return ERR_CODE_ALREADY_FORMATTED;
    }
    
    // Format the storage (writes VERSION file)
    storage.format(nsInfo);

    // Load the newly formatted image, using all of the directories (including shared
    // edits)
    FSImage image = new FSImage(conf);
    try {
      image.getStorage().setStorageInfo(storage);
      image.initEditLog(StartupOption.REGULAR);
      assert image.getEditLog().isOpenForRead() :
        "Expected edit log to be open for read";

      // Ensure that we have enough edits already in the shared directory to
      // start up from the last checkpoint on the active.
      if (!skipSharedEditsCheck && !checkLogsAvailableForRead(image, imageTxId, curTxId)) {
        return ERR_CODE_LOGS_UNAVAILABLE;
      }

      image.getStorage().writeTransactionIdFileToStorage(curTxId);

      // Download that checkpoint into our storage directories.
      MD5Hash hash = TransferFsImage.downloadImageToStorage(
        proxyInfo.getHttpAddress(), imageTxId, storage, true);
      image.saveDigestAndRenameCheckpointImage(NameNodeFile.IMAGE, imageTxId,
          hash);
    } catch (IOException ioe) {
      image.close();
      throw ioe;
    }
    return 0;
  }

  private boolean checkLogsAvailableForRead(FSImage image, long imageTxId,
      long curTxIdOnOtherNode) {

    if (imageTxId == curTxIdOnOtherNode) {
      // The other node hasn't written any logs since the last checkpoint.
      // This can be the case if the NN was freshly formatted as HA, and
      // then started in standby mode, so it has no edit logs at all.
      return true;
    }
    long firstTxIdInLogs = imageTxId + 1;
    
    assert curTxIdOnOtherNode >= firstTxIdInLogs :
      "first=" + firstTxIdInLogs + " onOtherNode=" + curTxIdOnOtherNode;
    
    try {
      Collection<EditLogInputStream> streams =
        image.getEditLog().selectInputStreams(
          firstTxIdInLogs, curTxIdOnOtherNode, null, true);
      for (EditLogInputStream stream : streams) {
        IOUtils.closeStream(stream);
      }
      return true;
    } catch (IOException e) {
      String msg = "Unable to read transaction ids " +
          firstTxIdInLogs + "-" + curTxIdOnOtherNode +
          " from the configured shared edits storage " +
          Joiner.on(",").join(sharedEditsUris) + ". " +
          "Please copy these logs into the shared edits storage " + 
          "or call saveNamespace on the active node.\n" +
          "Error: " + e.getLocalizedMessage();
      if (LOG.isDebugEnabled()) {
        LOG.fatal(msg, e);
      } else {
        LOG.fatal(msg);
      }
      return false;
    }
  }

  private boolean checkLayoutVersion(NamespaceInfo nsInfo) throws IOException {
    return (nsInfo.getLayoutVersion() == HdfsConstants.NAMENODE_LAYOUT_VERSION);
  }
  
  private void parseConfAndFindOtherNN() throws IOException {
    Configuration conf = getConf();
    nsId = DFSUtil.getNamenodeNameServiceId(conf);

    if (!HAUtil.isHAEnabled(conf, nsId)) {
      throw new HadoopIllegalArgumentException(
          "HA is not enabled for this namenode.");
    }
    nnId = HAUtil.getNameNodeId(conf, nsId);
    NameNode.initializeGenericKeys(conf, nsId, nnId);

    if (!HAUtil.usesSharedEditsDir(conf)) {
      throw new HadoopIllegalArgumentException(
        "Shared edits storage is not enabled for this namenode.");
    }


    remoteNNs = RemoteNameNodeInfo.getRemoteNameNodes(conf, nsId);
    // validate the configured NNs
    List<RemoteNameNodeInfo> remove = new ArrayList<RemoteNameNodeInfo>(remoteNNs.size());
    for (RemoteNameNodeInfo info : remoteNNs) {
      InetSocketAddress address = info.getIpcAddress();
      LOG.info("Found nn: " + info.getNameNodeID() + ", ipc: " + info.getIpcAddress());
      if (address.getPort() == 0 || address.getAddress().isAnyLocalAddress()) {
        LOG.error("Could not determine valid IPC address for other NameNode ("
            + info.getNameNodeID() + ") , got: " + address);
        remove.add(info);
      }
    }

    // remove any invalid nns
    remoteNNs.removeAll(remove);

    // make sure we have at least one left to read
    Preconditions.checkArgument(!remoteNNs.isEmpty(), "Could not find any valid namenodes!");

    dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    editUrisToFormat = FSNamesystem.getNamespaceEditsDirs(
        conf, false);
    sharedEditsUris = FSNamesystem.getSharedEditsDirs(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = DFSHAAdmin.addSecurityConfiguration(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
  
  public static int run(String[] argv, Configuration conf) throws IOException {
    BootstrapStandby bs = new BootstrapStandby();
    bs.setConf(conf);
    try {
      return ToolRunner.run(bs, argv);
    } catch (Exception e) {
      if (e instanceof IOException) {
        throw (IOException)e;
      } else {
        throw new IOException(e);
      }
    }
  }
}
