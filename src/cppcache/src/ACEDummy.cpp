/*
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
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <ace/File_Lock.h>
#include <ace/Process.h>
#include <ace/SPIPE_Addr.h>

namespace gemfire {

class CPPCACHE_EXPORT ACEDummy {
 public:
  static void useUnusedAceFeatures();
};

void ACEDummy::useUnusedAceFeatures() {
  ACE_File_Lock fLock("/BadFileName", 0);
  ACE_Process proc;
  ACE_SPIPE_Addr addr;
}
}  // namespace gemfire
