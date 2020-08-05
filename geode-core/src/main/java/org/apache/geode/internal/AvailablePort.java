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
package org.apache.geode.internal;

import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Random;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This class determines whether or not a given port is available and can also provide a randomly
 * selected available port.
 */
public class AvailablePort {
  private static final Logger logger = LogService.getLogger();


  /**
   * Is the port available for a Socket (TCP) connection?
   */
  public static final int SOCKET = 0;
  public static final int AVAILABLE_PORTS_LOWER_BOUND = 20001;// 20000/udp is securid
  public static final int AVAILABLE_PORTS_UPPER_BOUND = 29999;// 30000/tcp is spoolfax
  /**
   * Is the port available for a JGroups (UDP) multicast connection
   */
  public static final int MULTICAST = 1;

  /////////////////////// Static Methods ///////////////////////

  /**
   * see if there is a gemfire system property that establishes a default address for the given
   * protocol, and return it
   */
  public static InetAddress getAddress(int protocol) {
    String name = null;
    try {
      if (protocol == SOCKET) {
        name = System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "bind-address");
      } else if (protocol == MULTICAST) {
        name = System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "mcast-address");
      }
      if (name != null) {
        return InetAddress.getByName(name);
      }
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to resolve address " + name);
    }
    return null;
  }


  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @throws IllegalArgumentException <code>protocol</code> is unknown
   */
  public static boolean isPortAvailable(final int port, int protocol) {
    return isPortAvailable(port, protocol, getAddress(protocol));
  }


  /**
   * Returns whether or not the given port on the local host is available (that is, unused).
   *
   * @param port The port to check
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind address (or mcast address) to use
   * @throws IllegalArgumentException <code>protocol</code> is unknown
   */
  public static boolean isPortAvailable(final int port, int protocol, InetAddress addr) {
    if (protocol == SOCKET) {
      // Try to create a ServerSocket
      if (addr == null) {
        return testAllInterfaces(port);
      } else {
        return testOneInterface(addr, port);
      }
    } else if (protocol == MULTICAST) {
      MulticastSocket socket = null;
      try {
        socket = new MulticastSocket();
        InetAddress localHost = LocalHostUtil.getLocalHost();
        socket.setInterface(localHost);
        socket.setSoTimeout(Integer.getInteger("AvailablePort.timeout", 2000).intValue());
        socket.setReuseAddress(true);
        byte[] buffer = new byte[4];
        buffer[0] = (byte) 'p';
        buffer[1] = (byte) 'i';
        buffer[2] = (byte) 'n';
        buffer[3] = (byte) 'g';
        InetAddress mcid = addr == null ? DistributionConfig.DEFAULT_MCAST_ADDRESS : addr;
        SocketAddress mcaddr = new InetSocketAddress(mcid, port);
        socket.joinGroup(mcid);
        DatagramPacket packet = new DatagramPacket(buffer, 0, buffer.length, mcaddr);
        socket.send(packet);
        try {
          socket.receive(packet);
          packet.getData(); // make sure there's data, but no need to process it
          return false;
        } catch (SocketTimeoutException ste) {
          // System.out.println("socket read timed out");
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        }
      } catch (java.io.IOException ioe) {
        if (ioe.getMessage().equals("Network is unreachable")) {
          throw new RuntimeException(
              "Network is unreachable", ioe);
        }
        ioe.printStackTrace();
        return false;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      } finally {
        if (socket != null) {
          try {
            socket.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    } else {
      throw new IllegalArgumentException(String.format("Unknown protocol: %s",
          Integer.valueOf(protocol)));
    }
  }

  private static boolean testOneInterface(InetAddress addr, int port) {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket();
      socket.setReuseAddress(true);
      if (addr != null) {
        socket.bind(new InetSocketAddress(addr, port));
      } else {
        socket.bind(new InetSocketAddress(port));
      }
      return true;
    } catch (java.io.IOException ioe) {
      if (ioe.getMessage().equals("Network is unreachable")) {
        throw new RuntimeException("Network is unreachable");
      }
      // ioe.printStackTrace();
      if (addr instanceof Inet6Address) {
        byte[] addrBytes = addr.getAddress();
        if ((addrBytes[0] == (byte) 0xfe) && (addrBytes[1] == (byte) 0x80)) {
          // Hack, early Sun 1.5 versions (like Hitachi's JVM) cannot handle IPv6
          // link local addresses. Cannot trust InetAddress.isLinkLocalAddress()
          // see http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6558853
          // By returning true we ignore these interfaces and potentially say a
          // port is not in use when it really is.
          return true;
        }
      }
      return false;
    } catch (Exception ex) {
      return false;
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (Exception ex) {

        }
      }
    }
  }


  /**
   * Test to see if a given port is available port on all interfaces on this host.
   *
   * @return true of if the port is free on all interfaces
   */
  private static boolean testAllInterfaces(int port) {
    // First check to see if we can bind to the wildcard address.
    if (!testOneInterface(null, port)) {
      return false;
    }

    // Now check all of the addresses for all of the addresses
    // on this system. On some systems (solaris, aix) binding
    // to the wildcard address will successfully bind to only some
    // of the interfaces if other interfaces are in use. We want to
    // make sure this port is completely free.
    //
    // Note that we still need the check of the wildcard address, above,
    // because on some systems (aix) we can still bind to specific addresses
    // if someone else has bound to the wildcard address.
    Enumeration en;
    try {
      en = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      throw new RuntimeException(e);
    }
    while (en.hasMoreElements()) {
      NetworkInterface next = (NetworkInterface) en.nextElement();
      Enumeration en2 = next.getInetAddresses();
      while (en2.hasMoreElements()) {
        InetAddress addr = (InetAddress) en2.nextElement();
        boolean available = testOneInterface(addr, port);
        if (!available) {
          return false;
        }
      }
    }
    // Now do it one more time but reserve the wildcard address
    return testOneInterface(null, port);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @throws IllegalArgumentException <code>protocol</code> is unknown
   */
  public static int getRandomAvailablePort(int protocol) {
    return getRandomAvailablePort(protocol, getAddress(protocol));
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind-address or mcast address to use
   * @throws IllegalArgumentException <code>protocol</code> is unknown
   */
  public static int getRandomAvailablePort(int protocol, InetAddress addr) {
    return getRandomAvailablePort(protocol, addr, false);
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767.
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind-address or mcast address to use
   * @param useMembershipPortRange use true if the port will be used for membership
   * @throws IllegalArgumentException <code>protocol</code> is unknown
   */
  public static int getRandomAvailablePort(int protocol, InetAddress addr,
      boolean useMembershipPortRange) {
    int fails = 0;
    while (true) {
      int port = getRandomWildcardBindPortNumber(useMembershipPortRange);
      if (isPortAvailable(port, protocol, addr)) {
        // don't return the products default multicast port
        if (!(protocol == MULTICAST && port == DistributionConfig.DEFAULT_MCAST_PORT)) {
          logger.info(
              "DHE: reserved port {} AP.getRandomAvailablePort({},{},{})" +
                  " after testing {} unavailable ports",
              port, protocol, addr, useMembershipPortRange, fails);
          return port;
        }
      }
      fails++;
    }
  }

  /**
   * Returns a randomly selected available port in the provided range.
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind-address or mcast address to use
   * @throws IllegalArgumentException <code>protocol</code> is unknown
   */
  public static int getAvailablePortInRange(int protocol, InetAddress addr, int rangeBase,
      int rangeTop) {
    for (int port = rangeBase; port <= rangeTop; port++) {
      if (isPortAvailable(port, protocol, addr)) {
        logger.info("DHE: reserved port {} AP.getRandomAvailablePortInRange(,{},{},{},{}) " +
            " after testing {} unavailable ports",
            port, protocol, addr, rangeBase, rangeTop, port - rangeBase);
        return port;
      }
    }
    logger.info("DHE: AP.getAvailablePortInRange({},{},{},{}) FAILED to reserve a port",
        protocol, addr, rangeBase, rangeTop);
    return -1;
  }

  /**
   * Returns a randomly selected available port in the range 5001 to 32767 that satisfies a modulus
   * and the provided protocol
   *
   * @param protocol The protocol to check (either {@link #SOCKET} or {@link #MULTICAST}).
   * @param addr the bind-address or mcast address to use
   * @throws IllegalArgumentException <code>protocol</code> is unknown
   */
  public static int getRandomAvailablePortWithMod(int protocol, InetAddress addr, int mod) {
    int fails = 0;
    while (true) {
      int port = getRandomWildcardBindPortNumber();
      if (isPortAvailable(port, protocol, addr) && (port % mod) == 0) {
        logger.info(
            "DHE: reserved port {} AP.getRandomAvailablePortWithMod({},{},{})"
                + " after testing {} unavailable or unsatisfactory ports",
            port, protocol, addr, mod, fails);
        return port;
      }
      fails++;
    }
  }


  @Immutable
  public static final Random rand;

  static {
    boolean fast = Boolean.getBoolean("AvailablePort.fastRandom");
    if (fast) {
      rand = new Random();
    } else {
      rand = new java.security.SecureRandom();
    }
  }

  private static int getRandomWildcardBindPortNumber() {
    return getRandomWildcardBindPortNumber(false);
  }

  private static int getRandomWildcardBindPortNumber(boolean useMembershipPortRange) {
    int rangeBase;
    int rangeTop;
    if (!useMembershipPortRange) {
      rangeBase = AVAILABLE_PORTS_LOWER_BOUND; // 20000/udp is securid
      rangeTop = AVAILABLE_PORTS_UPPER_BOUND; // 30000/tcp is spoolfax
    } else {
      rangeBase = DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[0];
      rangeTop = DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE[1];
    }

    return rand.nextInt(rangeTop - rangeBase) + rangeBase;
  }

  private static int getRandomPortNumberInRange(int rangeBase, int rangeTop) {
    return rand.nextInt(rangeTop - rangeBase) + rangeBase;
  }

  public static int getRandomAvailablePortInRange(int rangeBase, int rangeTop, int protocol) {
    int numberOfPorts = rangeTop - rangeBase;
    // do "5 times the numberOfPorts" iterations to select a port number. This will ensure that
    // each of the ports from given port range get a chance at least once
    int numberOfRetrys = numberOfPorts * 5;
    int fails = 0;
    for (int i = 0; i < numberOfRetrys; i++) {
      int port = rand.nextInt(numberOfPorts + 1) + rangeBase;// add 1 to numberOfPorts so that
      // rangeTop also gets included
      if (isPortAvailable(port, protocol, getAddress(protocol))) {
        logger.info(
            "DHE: reserved port {} AP.getRandomAvailablePortInRange({},{},{})" +
                " after testing {} unavailable ports",
            port, rangeBase, rangeTop, protocol, fails);
        return port;
      }
      fails++;
    }
    logger.info("DHE: AP.getRandomAvailablePortInRange({},{},{}) FAILED to reserve a port",
        rangeBase, rangeTop, protocol);
    return -1;
  }

  /////////////////////// Main Program ///////////////////////

  @Immutable
  private static final PrintStream out = System.out;
  @Immutable
  private static final PrintStream err = System.err;

  private static void usage(String s) {
    err.println("\n** " + s + "\n");
    err.println("usage: java AvailablePort socket|jgroups [\"addr\" network-address] [port]");
    err.println("");
    err.println(
        "This program either prints whether or not a port is available for a given protocol, or it "
            + "prints out an available port for a given protocol.");
    err.println("");
    ExitCode.FATAL.doSystemExit();
  }

  public static void main(String[] args) {
    String protocolString = null;
    String addrString = null;
    String portString = null;

    for (int i = 0; i < args.length; i++) {
      if (protocolString == null) {
        protocolString = args[i];

      } else if (args[i].equals("addr") && i < args.length - 1) {
        addrString = args[++i];
      } else if (portString == null) {
        portString = args[i];
      } else {
        usage("Spurious command line: " + args[i]);
      }
    }

    int protocol;

    if (protocolString == null) {
      usage("Missing protocol");
      return;

    } else if (protocolString.equalsIgnoreCase("socket")) {
      protocol = SOCKET;

    } else if (protocolString.equalsIgnoreCase("javagroups")
        || protocolString.equalsIgnoreCase("jgroups")) {
      protocol = MULTICAST;

    } else {
      usage("Unknown protocol: " + protocolString);
      return;
    }

    InetAddress addr = null;
    if (addrString != null) {
      try {
        addr = InetAddress.getByName(addrString);
      } catch (Exception e) {
        e.printStackTrace();
        ExitCode.FATAL.doSystemExit();
      }
    }

    if (portString != null) {
      int port;
      try {
        port = Integer.parseInt(portString);

      } catch (NumberFormatException ex) {
        usage("Malformed port: " + portString);
        return;
      }

      out.println("\nPort " + port + " is " + (isPortAvailable(port, protocol, addr) ? "" : "not ")
          + "available for a " + protocolString + " connection\n");

    } else {
      out.println("\nRandomly selected " + protocolString + " port: "
          + getRandomAvailablePort(protocol, addr) + "\n");

    }

  }
}
