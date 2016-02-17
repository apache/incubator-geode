/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.vmware.gemfire.tools.pulse.internal.service;

import javax.servlet.http.HttpServletRequest;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.vmware.gemfire.tools.pulse.internal.data.Cluster;
import com.vmware.gemfire.tools.pulse.internal.data.Repository;
import com.vmware.gemfire.tools.pulse.internal.json.JSONArray;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class MemberGCPausesService
 * 
 * This class contains implementations of getting Memeber's GC Pauses (JVM
 * Pauses) details and its trend over the time.
 * 
 * @since version 7.5
 */

@Component
@Service("MemberGCPauses")
@Scope("singleton")
public class MemberGCPausesService implements PulseService {

  public JSONObject execute(final HttpServletRequest request) throws Exception {

    // get cluster object
    Cluster cluster = Repository.get().getCluster();

    // json object to be sent as response
    JSONObject responseJSON = new JSONObject();

    // members list
    try {

      JSONObject requestDataJSON = new JSONObject(
          request.getParameter("pulseData"));
      String memberName = requestDataJSON.getJSONObject("MemberGCPauses")
          .getString("memberName");
      Cluster.Member clusterMember = cluster.getMember(StringUtils
          .makeCompliantName(memberName));
      if (clusterMember != null) {
        // response
        responseJSON
            .put(
                "gcPausesTrend",
                new JSONArray(
                    clusterMember
                        .getMemberStatisticTrend(Cluster.Member.MEMBER_STAT_GARBAGE_COLLECTION)));
        responseJSON.put("gcPausesCount",
            clusterMember.getGarbageCollectionCount());
      }

      // Send json response
      return responseJSON;
    } catch (JSONException e) {
      throw new Exception(e);
    }
  }
}
