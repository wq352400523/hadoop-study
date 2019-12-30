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

package org.apache.hadoop.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a way to access information about the map/reduce cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Cluster {
  
  @InterfaceStability.Evolving
  public enum JobTrackerStatus {INITIALIZING, RUNNING};

  /**
   * 用于根据配置内容判断是否生成对应的ClientProtocol实例
   */
  private ClientProtocolProvider clientProtocolProvider;
  /**
   * 核心属性：是mr client与yarn（集群模式下）交互的媒介
   */
  private ClientProtocol client;
  private UserGroupInformation ugi;
  private Configuration conf;
  private FileSystem fs = null;
  private Path sysDir = null;
  private Path stagingAreaDir = null;
  private Path jobHistoryDir = null;
  private static final Logger LOG =
      LoggerFactory.getLogger(Cluster.class);

  /**
   * ServiceLoader：加载资源目录中/META-INF下的jar文件，获取相应的类实例（一般是子类）
   * 这个有点类似于spring 里的@service自动装载bean
   */
  @VisibleForTesting
  static Iterable<ClientProtocolProvider> frameworkLoader =
      ServiceLoader.load(ClientProtocolProvider.class);
  private volatile List<ClientProtocolProvider> providerList = null;

  /**
   * 读/META-INF下的配置，生成ClientProtocolProvider实例
   */
  private void initProviderList() {
    if (providerList == null) {
      synchronized (frameworkLoader) {
        if (providerList == null) {
          List<ClientProtocolProvider> localProviderList =
              new ArrayList<ClientProtocolProvider>();
          try {
            for (ClientProtocolProvider provider : frameworkLoader) {
              localProviderList.add(provider);
            }
          } catch(ServiceConfigurationError e) {
            LOG.info("Failed to instantiate ClientProtocolProvider, please "
                         + "check the /META-INF/services/org.apache."
                         + "hadoop.mapreduce.protocol.ClientProtocolProvider "
                         + "files on the classpath", e);
          }
          providerList = localProviderList;
        }
      }
    }
  }

  /**
   * 添加配置文件
   */
  static {
    ConfigUtil.loadResources();
  }
  
  public Cluster(Configuration conf) throws IOException {
    this(null, conf);
  }

  /**
   * 先是static静态段加载配置文件，以确定是本地模式还是集群模式、集群模式的yarn ip等等
   * 然后调用{ initialize}函数初始化
   *
   * @param jobTrackAddr
   * @param conf
   * @throws IOException
   */
  public Cluster(InetSocketAddress jobTrackAddr, Configuration conf) 
      throws IOException {
    this.conf = conf;
    this.ugi = UserGroupInformation.getCurrentUser();
    initialize(jobTrackAddr, conf);
  }

  /**
   * 获取ClientProtocol和ClientProtocolProvider的实例
   * @param jobTrackAddr
   * @param conf
   * @throws IOException
   */
  private void initialize(InetSocketAddress jobTrackAddr, Configuration conf)
      throws IOException {

    /**
     * 获取ClientProtocolProvider类的实例（可能有多个，只要配置在了/META-INF/services/目录下）
     * frameworkLoader 一般有两个ClientProtocolProvider类的子类实例：YarnClientProtocolProvider和LocalClientProtocolProvider
     * 每个Provider依次尝试，看conf中的mapreduce.framework.name对应的值是否是当前Provider对应的值；如果是；返回对应的
     * ClientProtocol实例，然后break遍历；否则继续遍历frameworkLoader直到找到对应的ClientProtocolProvider。
     */
    initProviderList();
    final IOException initEx = new IOException(
        "Cannot initialize Cluster. Please check your configuration for "
            + MRConfig.FRAMEWORK_NAME
            + " and the correspond server addresses.");
    if (jobTrackAddr != null) {
      LOG.info(
          "Initializing cluster for Job Tracker=" + jobTrackAddr.toString());
    }
    /**
     * 虽然是list，但是只会使用一个
     */
    for (ClientProtocolProvider provider : providerList) {
      LOG.debug("Trying ClientProtocolProvider : "
          + provider.getClass().getName());
      ClientProtocol clientProtocol = null;
      try {
        if (jobTrackAddr == null) {
          clientProtocol = provider.create(conf);
        } else {
          clientProtocol = provider.create(jobTrackAddr, conf);
        }

        if (clientProtocol != null) {
          clientProtocolProvider = provider;
          client = clientProtocol;
          LOG.debug("Picked " + provider.getClass().getName()
              + " as the ClientProtocolProvider");
          break;
        } else {
          LOG.debug("Cannot pick " + provider.getClass().getName()
              + " as the ClientProtocolProvider - returned null protocol");
        }
      } catch (Exception e) {
        final String errMsg = "Failed to use " + provider.getClass().getName()
            + " due to error: ";
        initEx.addSuppressed(new IOException(errMsg, e));
        LOG.info(errMsg, e);
      }
    }

    if (null == clientProtocolProvider || null == client) {
      throw initEx;
    }
  }

  ClientProtocol getClient() {
    return client;
  }
  
  Configuration getConf() {
    return conf;
  }
  
  /**
   * Close the <code>Cluster</code>.
   * @throws IOException
   */
  public synchronized void close() throws IOException {
    clientProtocolProvider.close(client);
  }

  private Job[] getJobs(JobStatus[] stats) throws IOException {
    List<Job> jobs = new ArrayList<Job>();
    for (JobStatus stat : stats) {
      jobs.add(Job.getInstance(this, stat, new JobConf(stat.getJobFile())));
    }
    return jobs.toArray(new Job[0]);
  }

  /**
   * Get the file system where job-specific files are stored
   * 
   * @return object of FileSystem
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized FileSystem getFileSystem() 
      throws IOException, InterruptedException {
    if (this.fs == null) {
      try {
        this.fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException, InterruptedException {
            final Path sysDir = new Path(client.getSystemDir());
            return sysDir.getFileSystem(getConf());
          }
        });
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return fs;
  }

  /**
   * Get job corresponding to jobid.
   * 
   * @param jobId
   * @return object of {@link Job}
   * @throws IOException
   * @throws InterruptedException
   */
  public Job getJob(JobID jobId) throws IOException, InterruptedException {
    JobStatus status = client.getJobStatus(jobId);
    if (status != null) {
      JobConf conf;
      try {
        conf = new JobConf(status.getJobFile());
      } catch (RuntimeException ex) {
        // If job file doesn't exist it means we can't find the job
        if (ex.getCause() instanceof FileNotFoundException) {
          return null;
        } else {
          throw ex;
        }
      }
      return Job.getInstance(this, status, conf);
    }
    return null;
  }
  
  /**
   * Get all the queues in cluster.
   * 
   * @return array of {@link QueueInfo}
   * @throws IOException
   * @throws InterruptedException
   */
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    return client.getQueues();
  }
  
  /**
   * Get queue information for the specified name.
   * 
   * @param name queuename
   * @return object of {@link QueueInfo}
   * @throws IOException
   * @throws InterruptedException
   */
  public QueueInfo getQueue(String name) 
      throws IOException, InterruptedException {
    return client.getQueue(name);
  }

  /**
   * Get log parameters for the specified jobID or taskAttemptID
   * @param jobID the job id.
   * @param taskAttemptID the task attempt id. Optional.
   * @return the LogParams
   * @throws IOException
   * @throws InterruptedException
   */
  public LogParams getLogParams(JobID jobID, TaskAttemptID taskAttemptID)
      throws IOException, InterruptedException {
    return client.getLogFileParams(jobID, taskAttemptID);
  }

  /**
   * Get current cluster status.
   * 
   * @return object of {@link ClusterMetrics}
   * @throws IOException
   * @throws InterruptedException
   */
  public ClusterMetrics getClusterStatus() throws IOException, InterruptedException {
    return client.getClusterMetrics();
  }
  
  /**
   * Get all active trackers in the cluster.
   * 
   * @return array of {@link TaskTrackerInfo}
   * @throws IOException
   * @throws InterruptedException
   */
  public TaskTrackerInfo[] getActiveTaskTrackers() 
      throws IOException, InterruptedException  {
    return client.getActiveTrackers();
  }
  
  /**
   * Get blacklisted trackers.
   * 
   * @return array of {@link TaskTrackerInfo}
   * @throws IOException
   * @throws InterruptedException
   */
  public TaskTrackerInfo[] getBlackListedTaskTrackers() 
      throws IOException, InterruptedException  {
    return client.getBlacklistedTrackers();
  }
  
  /**
   * Get all the jobs in cluster.
   * 
   * @return array of {@link Job}
   * @throws IOException
   * @throws InterruptedException
   * @deprecated Use {@link #getAllJobStatuses()} instead.
   */
  @Deprecated
  public Job[] getAllJobs() throws IOException, InterruptedException {
    return getJobs(client.getAllJobs());
  }

  /**
   * Get job status for all jobs in the cluster.
   * @return job status for all jobs in cluster
   * @throws IOException
   * @throws InterruptedException
   */
  public JobStatus[] getAllJobStatuses() throws IOException, InterruptedException {
    return client.getAllJobs();
  }

  /**
   * Grab the jobtracker system directory path where 
   * job-specific files will  be placed.
   * 
   * @return the system directory where job-specific files are to be placed.
   */
  public Path getSystemDir() throws IOException, InterruptedException {
    if (sysDir == null) {
      sysDir = new Path(client.getSystemDir());
    }
    return sysDir;
  }
  
  /**
   * Grab the jobtracker's view of the staging directory path where 
   * job-specific files will  be placed.
   * 
   * @return the staging directory where job-specific files are to be placed.
   */
  public Path getStagingAreaDir() throws IOException, InterruptedException {
    if (stagingAreaDir == null) {
      stagingAreaDir = new Path(client.getStagingAreaDir());
    }
    return stagingAreaDir;
  }

  /**
   * Get the job history file path for a given job id. The job history file at 
   * this path may or may not be existing depending on the job completion state.
   * The file is present only for the completed jobs.
   * @param jobId the JobID of the job submitted by the current user.
   * @return the file path of the job history file
   * @throws IOException
   * @throws InterruptedException
   */
  public String getJobHistoryUrl(JobID jobId) throws IOException, 
    InterruptedException {
    if (jobHistoryDir == null) {
      jobHistoryDir = new Path(client.getJobHistoryDir());
    }
    return new Path(jobHistoryDir, jobId.toString() + "_"
                    + ugi.getShortUserName()).toString();
  }

  /**
   * Gets the Queue ACLs for current user
   * @return array of QueueAclsInfo object for current user.
   * @throws IOException
   */
  public QueueAclsInfo[] getQueueAclsForCurrentUser() 
      throws IOException, InterruptedException  {
    return client.getQueueAclsForCurrentUser();
  }

  /**
   * Gets the root level queues.
   * @return array of JobQueueInfo object.
   * @throws IOException
   */
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    return client.getRootQueues();
  }
  
  /**
   * Returns immediate children of queueName.
   * @param queueName
   * @return array of JobQueueInfo which are children of queueName
   * @throws IOException
   */
  public QueueInfo[] getChildQueues(String queueName) 
      throws IOException, InterruptedException {
    return client.getChildQueues(queueName);
  }
  
  /**
   * Get the JobTracker's status.
   * 
   * @return {@link JobTrackerStatus} of the JobTracker
   * @throws IOException
   * @throws InterruptedException
   */
  public JobTrackerStatus getJobTrackerStatus() throws IOException,
      InterruptedException {
    return client.getJobTrackerStatus();
  }
  
  /**
   * Get the tasktracker expiry interval for the cluster
   * @return the expiry interval in msec
   */
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return client.getTaskTrackerExpiryInterval();
  }

  /**
   * Get a delegation token for the user from the JobTracker.
   * @param renewer the user who can renew the token
   * @return the new token
   * @throws IOException
   */
  public Token<DelegationTokenIdentifier> 
      getDelegationToken(Text renewer) throws IOException, InterruptedException{
    // client has already set the service
    return client.getDelegationToken(renewer);
  }

  /**
   * Renew a delegation token
   * @param token the token to renew
   * @return the new expiration time
   * @throws InvalidToken
   * @throws IOException
   * @deprecated Use {@link Token#renew} instead
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                   ) throws InvalidToken, IOException,
                                            InterruptedException {
    return token.renew(getConf());
  }

  /**
   * Cancel a delegation token from the JobTracker
   * @param token the token to cancel
   * @throws IOException
   * @deprecated Use {@link Token#cancel} instead
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                    ) throws IOException,
                                             InterruptedException {
    token.cancel(getConf());
  }

}
