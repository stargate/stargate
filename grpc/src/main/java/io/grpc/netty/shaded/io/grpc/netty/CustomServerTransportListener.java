package io.grpc.netty.shaded.io.grpc.netty;

import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.internal.ServerStream;
import io.grpc.internal.ServerTransportListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomServerTransportListener implements ServerTransportListener {
  private static final Logger logger = LoggerFactory.getLogger(CustomServerTransportListener.class);
  // todo maybe switch order to ServerStream -> Metadata
  private final Map<String, List<ServerStream>> serverStreamsPerHeader = new ConcurrentHashMap<>();

  @Override
  public void streamCreated(ServerStream stream, String method, Metadata headers) {
    System.out.println(
        "Stargate, new stream created: " + stream + " method: " + method + " headers: " + headers);

    System.out.println("Extracting authority from NettyServerStream");
    String host = "";

    String authority = stream.getAuthority();
    if (authority != null && !authority.isEmpty()) {
      host = authority;
    }

    System.out.println("adding stringHeaders for a stream, host: " + host);
    serverStreamsPerHeader.compute(
        host,
        (h, streams) -> {
          if (streams == null) {
            streams = new ArrayList<>();
          }
          System.out.println("Size of streams: " + streams.size() + " for host:" + h);
          streams.add(stream);

          return streams;
        });
  }

  @Override
  public Attributes transportReady(Attributes attributes) {
    System.out.println("Stargate, transportReady: " + attributes);
    return attributes;
  }

  @Override
  public void transportTerminated() {
    System.out.println("Stargate, transportTerminated");
  }

  public void closeFilter(Predicate<Map<String, String>> headerFilter) {
    serverStreamsPerHeader.entrySet().stream()
        .filter(
            entry -> {
              Map<String, String> headers = new HashMap<>();
              headers.put("host", entry.getKey());
              logger.info(
                  "CustomServerTransportListener.closeFilter(): nr of streams: "
                      + serverStreamsPerHeader.size()
                      + " headers: "
                      + headers
                      + "Thread: "
                      + Thread.currentThread().getName());
              return headerFilter.test(headers);
            })
        .forEach(
            e -> {
              logger.info(
                  "Closing streams for headers: {} number of streams: {} all streams: {}",
                  e.getKey(),
                  e.getValue().size(),
                  e.getValue());

              e.getValue().stream()
                  .filter(v -> v instanceof NettyServerStream)
                  .map(v -> (NettyServerStream) v)
                  .forEach(
                      stream -> {
                        NettyServerStream.TransportState transportState = stream.transportState();
                        logger.info("transportState: " + transportState);
                        logger.info("Closing Stream: " + stream);
                        stream.close(
                            Status.UNAVAILABLE.withDescription("Stream closed"), new Metadata());
                        // todo
                        // if(!transportState.getState().equals(CLOSED)){

                        //                         }
                      });
            });
  }
}
