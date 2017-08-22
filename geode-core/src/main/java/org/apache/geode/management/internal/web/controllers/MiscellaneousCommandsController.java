/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.web.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;

/**
 * The MiscellaneousCommandsController class implements GemFire Management REST API web service
 * endpoints for the Gfsh Miscellaneous Commands.
 * <p/>
 * 
 * @see org.apache.geode.management.internal.cli.commands.MiscellaneousCommands
 * @see org.apache.geode.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since GemFire 8.0
 */
@Controller("miscellaneousController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class MiscellaneousCommandsController extends AbstractCommandsController {
  // TODO determine whether Async functionality is required
  @RequestMapping(method = RequestMethod.GET, value = "/stacktraces")
  @ResponseBody
  public String exportStackTraces(
      @RequestParam(value = CliStrings.EXPORT_STACKTRACE__FILE, required = false) final String file,
      @RequestParam(value = CliStrings.GROUP, required = false) final String groupName,
      @RequestParam(value = CliStrings.MEMBER, required = false) final String memberNameId,
      @RequestParam(value = CliStrings.EXPORT_STACKTRACE__FAIL__IF__FILE__PRESENT,
          required = false) final boolean failIfFilePresent) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);

    if (hasValue(file)) {
      command.addOption(CliStrings.EXPORT_STACKTRACE__FILE, decode(file));
    }

    if (hasValue(groupName)) {
      command.addOption(CliStrings.GROUP, groupName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.MEMBER, memberNameId);
    }

    if (hasValue(failIfFilePresent)) {
      command.addOption(CliStrings.EXPORT_STACKTRACE__FAIL__IF__FILE__PRESENT,
          String.valueOf(failIfFilePresent));
    }

    return processCommand(command.toString());
  }

  // TODO add Async functionality
  @RequestMapping(method = RequestMethod.POST, value = "/gc")
  @ResponseBody
  public String gc(
      @RequestParam(value = CliStrings.GROUP, required = false) final String[] groups) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.GC);

    if (hasValue(groups)) {
      command.addOption(CliStrings.GROUP, StringUtils.join(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  // TODO add Async functionality
  @RequestMapping(method = RequestMethod.POST, value = "/members/{member}/gc")
  @ResponseBody
  public String gc(@PathVariable("member") final String memberNameId) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.GC);
    command.addOption(CliStrings.MEMBER, decode(memberNameId));
    return processCommand(command.toString());
  }

  // TODO add Async functionality
  @RequestMapping(method = RequestMethod.GET, value = "/netstat")
  @ResponseBody
  public String netstat(
      @RequestParam(value = CliStrings.MEMBER, required = false) final String[] members,
      @RequestParam(value = CliStrings.GROUP, required = false) final String group,
      @RequestParam(value = CliStrings.NETSTAT__FILE, required = false) final String file,
      @RequestParam(value = CliStrings.NETSTAT__WITHLSOF,
          defaultValue = "false") final Boolean withLsof) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.NETSTAT);

    addCommandOption(null, command, CliStrings.MEMBER, members);
    addCommandOption(null, command, CliStrings.GROUP, group);
    addCommandOption(null, command, CliStrings.NETSTAT__FILE, file);
    addCommandOption(null, command, CliStrings.NETSTAT__WITHLSOF, withLsof);

    return processCommand(command.toString());
  }

  // TODO determine if Async functionality is required
  @RequestMapping(method = RequestMethod.GET, value = "/deadlocks")
  @ResponseBody
  public String showDeadLock(
      @RequestParam(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE) final String dependenciesFile) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHOW_DEADLOCK);
    command.addOption(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE, decode(dependenciesFile));
    return processCommand(command.toString());
  }

  // TODO determine if Async functionality is required
  @RequestMapping(method = RequestMethod.GET, value = "/members/{member}/log")
  @ResponseBody
  public String showLog(@PathVariable("member") final String memberNameId,
      @RequestParam(value = CliStrings.SHOW_LOG_LINE_NUM, defaultValue = "0") final Integer lines) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHOW_LOG);

    command.addOption(CliStrings.MEMBER, decode(memberNameId));
    command.addOption(CliStrings.SHOW_LOG_LINE_NUM, String.valueOf(lines));

    return processCommand(command.toString());
  }

  // TODO determine if Async functionality is required
  @RequestMapping(method = RequestMethod.GET, value = "/metrics")
  @ResponseBody
  public String showMetrics(
      @RequestParam(value = CliStrings.MEMBER, required = false) final String memberNameId,
      @RequestParam(value = CliStrings.SHOW_METRICS__REGION,
          required = false) final String regionNamePath,
      @RequestParam(value = CliStrings.SHOW_METRICS__FILE, required = false) final String file,
      @RequestParam(value = CliStrings.SHOW_METRICS__CACHESERVER__PORT,
          required = false) final String cacheServerPort,
      @RequestParam(value = CliStrings.SHOW_METRICS__CATEGORY,
          required = false) final String[] categories) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHOW_METRICS);

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.MEMBER, memberNameId);
    }

    if (hasValue(regionNamePath)) {
      command.addOption(CliStrings.SHOW_METRICS__REGION, regionNamePath);
    }

    if (hasValue(file)) {
      command.addOption(CliStrings.SHOW_METRICS__FILE, file);
    }

    if (hasValue(cacheServerPort)) {
      command.addOption(CliStrings.SHOW_METRICS__CACHESERVER__PORT, cacheServerPort);
    }

    if (hasValue(categories)) {
      command.addOption(CliStrings.SHOW_METRICS__CATEGORY,
          StringUtils.join(categories, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/shutdown")
  @ResponseBody
  public String shutdown(
      @RequestParam(value = CliStrings.SHUTDOWN__TIMEOUT,
          defaultValue = "-1") final Integer timeout,
      @RequestParam(value = CliStrings.INCLUDE_LOCATORS,
          defaultValue = "false") final boolean includeLocators) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHUTDOWN);
    command.addOption(CliStrings.SHUTDOWN__TIMEOUT, String.valueOf(timeout));
    command.addOption(CliStrings.INCLUDE_LOCATORS, String.valueOf(includeLocators));
    return processCommand(command.toString());
  }

  // TODO determine whether the {groups} and {members} path variables corresponding to the --groups
  // and --members
  // command-line options in the 'change loglevel' Gfsh command actually accept multiple values,
  // and...
  // TODO if so, then change the groups and members method parameters to String[] types.
  // TODO If not, then these options should be renamed!

  @RequestMapping(method = RequestMethod.POST, value = "/groups/{groups}/loglevel")
  @ResponseBody
  public String changeLogLevelForGroups(@PathVariable("groups") final String groups,
      @RequestParam(value = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL
      ) final String logLevel) {
    return internalChangeLogLevel(groups, null, logLevel);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/members/{members}/loglevel")
  @ResponseBody
  public String changeLogLevelForMembers(@PathVariable("members") final String members,
      @RequestParam(value = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL
      ) final String logLevel) {
    return internalChangeLogLevel(null, members, logLevel);
  }

  @RequestMapping(method = RequestMethod.POST,
      value = "/members/{members}/groups/{groups}/loglevel")
  @ResponseBody
  public String changeLogLevelForMembersAndGroups(@PathVariable("members") final String members,
      @PathVariable("groups") final String groups,
      @RequestParam(value = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL) final String logLevel) {
    return internalChangeLogLevel(groups, members, logLevel);
  }

  // NOTE since "logLevel" is "required", then just set the option; no need to validate it's value.
  private String internalChangeLogLevel(final String groups, final String members,
      final String logLevel) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CHANGE_LOGLEVEL);

    command.addOption(CliStrings.CHANGE_LOGLEVEL__LOGLEVEL, decode(logLevel));

    if (hasValue(groups)) {
      command.addOption(CliStrings.GROUP, decode(groups));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.MEMBER, decode(members));
    }

    return processCommand(command.toString());
  }

}
