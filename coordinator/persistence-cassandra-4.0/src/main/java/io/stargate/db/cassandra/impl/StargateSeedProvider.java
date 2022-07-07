package io.stargate.db.cassandra.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SeedProvider;

public class StargateSeedProvider implements SeedProvider {
  private static final Integer DEFAULT_SEED_PORT =
      Integer.getInteger("stargate.40_seed_port_override", null);

  private final List<InetAddressAndPort> seeds;

  public StargateSeedProvider(Map<String, String> args) {
    if (!args.containsKey("seeds")) throw new ConfigurationException("seeds arg required");

    seeds =
        Arrays.stream(args.get("seeds").split(","))
            .map(
                s -> {
                  try {
                    return InetAddress.getAllByName(s.trim());
                  } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                  }
                })
            .flatMap(Arrays::stream)
            .map(s -> InetAddressAndPort.getByAddressOverrideDefaults(s, DEFAULT_SEED_PORT))
            .collect(Collectors.toList());
  }

  @Override
  public List<InetAddressAndPort> getSeeds() {
    return seeds;
  }
}
