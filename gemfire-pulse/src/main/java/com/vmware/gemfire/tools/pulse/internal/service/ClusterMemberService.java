/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.service;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.controllers.PulseController;
import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;
import com.vmware.gemfire.tools.pulse.internal.util.TimeUtils;

/**
 * Class ClusterMemberService
 * 
 * This class contains implementations of getting Cluster Member's details
 * 
 * @author Anchal G
 * @since version 7.5
 */
@Component
// @Service("ClusterMember")
@Service("ClusterMembers")
@Scope("singleton")
public class ClusterMemberService implements PulseService {

  private final String HEAP_USAGE = "heapUsage";

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    Cluster.Member[] clusterMembersList = cluster.getMembers();

    // create members json
    List<JSONObject> memberListJson = new ArrayList<JSONObject>();
    try {
      for (Cluster.Member clusterMember : clusterMembersList) {
        JSONObject memberJSON = new JSONObject();
        // getting members detail
        memberJSON.put("gemfireVersion", clusterMember.getGemfireVersion());
        memberJSON.put("memberId", clusterMember.getId());
        memberJSON.put("name", clusterMember.getName());
        memberJSON.put("host", clusterMember.getHost());

        List<String> serverGroups = clusterMember.getServerGroups();
        if(serverGroups.size() == 0){
          serverGroups = new ArrayList<String>();
          serverGroups.add(PulseConstants.DEFAULT_SERVER_GROUP);
        }
        memberJSON.put("serverGroups", serverGroups);
        
        List<String> redundancyZones = clusterMember.getRedundancyZones();
        if(redundancyZones.size() == 0){
          redundancyZones = new ArrayList<String>();
          redundancyZones.add(PulseConstants.DEFAULT_REDUNDANCY_ZONE);
        }
        memberJSON.put("redundancyZones", redundancyZones);

        DecimalFormat df2 = new DecimalFormat(
            PulseConstants.DECIMAL_FORMAT_PATTERN);

        long usedHeapSize = cluster.getUsedHeapSize();
        long currentHeap = clusterMember.getCurrentHeapSize();
        if (usedHeapSize > 0) {
          float heapUsage = ((float) currentHeap / (float) usedHeapSize) * 100;
          memberJSON
              .put(this.HEAP_USAGE, Double.valueOf(df2.format(heapUsage)));
        } else {
          memberJSON.put(this.HEAP_USAGE, 0);
        }
        Float currentCPUUsage = clusterMember.getCpuUsage();

        memberJSON.put("cpuUsage", Float.valueOf(df2.format(currentCPUUsage)));
        memberJSON.put("currentHeapUsage", clusterMember.getCurrentHeapSize());
        memberJSON.put("isManager", clusterMember.isManager());
        memberJSON.put("uptime",
            TimeUtils.convertTimeSecondsToHMS(clusterMember.getUptime()));
        memberJSON.put("loadAvg", clusterMember.getLoadAverage());
        memberJSON.put("sockets", clusterMember.getTotalFileDescriptorOpen());
        memberJSON.put("threads", clusterMember.getNumThreads());

        // Number of member clients
        if (PulseController.getPulseProductSupport().equalsIgnoreCase(
            PulseConstants.PRODUCT_NAME_SQLFIRE)){
          memberJSON.put("clients", clusterMember.getNumSqlfireClients());
        }else{
          memberJSON.put("clients", clusterMember.getMemberClientsHMap().size());
        }
        memberJSON.put("queues", clusterMember.getQueueBacklog());

        memberListJson.add(memberJSON);
      }
      // clucter's Members
      responseJSON.put("members", memberListJson);
      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
