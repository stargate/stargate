package io.stargate.db.datastore.common;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SeedProvider;

public class StargateSeedProvider implements SeedProvider
{
    private final List<InetAddress> seeds;

    public StargateSeedProvider(Map<String, String> args)
    {
        if (!args.containsKey("seeds"))
            throw new ConfigurationException("seeds arg required");

        seeds = Arrays.stream(args.get("seeds").split(","))
                .map(s -> {
                    try
                    {
                        return InetAddress.getAllByName(s.trim());
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                })
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
    }

    @Override
    public List<InetAddress> getSeeds()
    {
        return seeds;
    }
}
