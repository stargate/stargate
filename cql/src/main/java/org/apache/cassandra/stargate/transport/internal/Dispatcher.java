/*
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

package org.apache.cassandra.stargate.transport.internal;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import org.apache.cassandra.net.FrameEncoder;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.Flusher.FlushItem;
import org.apache.cassandra.stargate.transport.internal.messages.ErrorMessage;
import org.apache.cassandra.stargate.transport.internal.messages.EventMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class Dispatcher {
  /*private static final LocalAwareExecutorService requestExecutor =
  SHARED.newExecutor(
      TransportDescriptor.getNativeTransportMaxThreads(),
      TransportDescriptor::setNativeTransportMaxThreads,
      "transport",
      "Native-Transport-Requests");*/

  private static final ConcurrentMap<EventLoop, Flusher> flusherLookup = new ConcurrentHashMap<>();
  private final boolean useLegacyFlusher;

  /**
   * Takes a Channel, Request and the Response produced by processRequest and outputs a FlushItem
   * appropriate for the pipeline, which is specific to the protocol version. V5 and above will
   * produce FlushItem.Framed instances whereas earlier versions require FlushItem.Unframed. The
   * instances of these FlushItem subclasses are specialized to release resources in the right way
   * for the specific pipeline that produced them.
   */
  // TODO parameterize with FlushItem subclass
  interface FlushItemConverter {
    FlushItem<?> toFlushItem(Channel channel, Message.Request request, Message.Response response);
  }

  public Dispatcher(boolean useLegacyFlusher) {
    this.useLegacyFlusher = useLegacyFlusher;
  }

  public void dispatch(Channel channel, Message.Request request, FlushItemConverter forFlusher) {
    // requestExecutor.submit(() -> );
    processRequest(channel, request, forFlusher);
  }

  /**
   * Note: this method may be executed on the netty event loop, during initial protocol negotiation
   */
  static CompletableFuture<? extends Message.Response> processRequest(
      ServerConnection connection, Message.Request request) {
    long queryStartNanoTime = System.nanoTime();
    if (connection.getVersion().isGreaterOrEqualTo(ProtocolVersion.V4))
      ClientWarn.instance.captureWarnings();

    connection.validateNewMessage(request.type, connection.getVersion());

    Message.logger.trace("Received: {}, v={}", request, connection.getVersion());
    connection.requests.inc();
    CompletableFuture<? extends Message.Response> res = request.execute(queryStartNanoTime);

    return res.thenApply(
        response -> {
          response.setStreamId(request.getStreamId());
          response.setWarnings(ClientWarn.instance.getWarnings());
          response.attach(connection);
          connection.applyStateTransition(request.type, response.type);
          return response;
        });
  }

  /** Note: this method is not expected to execute on the netty event loop. */
  void processRequest(Channel channel, Message.Request request, FlushItemConverter forFlusher) {
    final CompletableFuture<? extends Message.Response> response;
    final ServerConnection connection;

    try {
      assert request.connection() instanceof ServerConnection;
      connection = (ServerConnection) request.connection();
      response = processRequest(connection, request);
      response.whenComplete(
          (res, error) -> {
            FlushItem<?> toFlush;
            if (error != null) {
              ExceptionHandlers.UnexpectedChannelExceptionHandler handler =
                  new ExceptionHandlers.UnexpectedChannelExceptionHandler(channel, true);
              ErrorMessage errorMessage = ErrorMessage.fromException(error, handler);
              errorMessage.setStreamId(request.getStreamId());
              toFlush = forFlusher.toFlushItem(channel, request, errorMessage);
            } else {
              toFlush = forFlusher.toFlushItem(channel, request, res);
            }
            flush(toFlush);
          });
    } catch (Throwable t) {
      JVMStabilityInspector.inspectThrowable(t);

    } finally {
      ClientWarn.instance.resetWarnings();
    }
  }

  private void flush(FlushItem<?> item) {
    EventLoop loop = item.channel.eventLoop();
    Flusher flusher = flusherLookup.get(loop);
    if (flusher == null) {
      Flusher created = useLegacyFlusher ? Flusher.legacy(loop) : Flusher.immediate(loop);
      Flusher alt = flusherLookup.putIfAbsent(loop, flusher = created);
      if (alt != null) flusher = alt;
    }

    flusher.enqueue(item);
    flusher.start();
  }

  public static void shutdown() {
    /*if (requestExecutor != null) {
      requestExecutor.shutdown();
    }*/
  }

  /**
   * Dispatcher for EventMessages. In {link Server.ConnectionTracker#send(Event)}, the strategy for
   * delivering events to registered clients is dependent on protocol version and the configuration
   * of the pipeline. For v5 and newer connections, the event message is encoded into an Envelope,
   * wrapped in a FlushItem and then delivered via the pipeline's flusher, in a similar way to a
   * Response returned from {@link #processRequest(Channel, Message.Request, FlushItemConverter)}.
   * It's worth noting that events are not generally fired as a direct response to a client request,
   * so this flush item has a null request attribute. The dispatcher itself is created when the
   * pipeline is first configured during protocol negotiation and is attached to the channel for
   * later retrieval.
   *
   * <p>Pre-v5 connections simply write the EventMessage directly to the pipeline.
   */
  static final AttributeKey<Consumer<EventMessage>> EVENT_DISPATCHER =
      AttributeKey.valueOf("EVTDISP");

  Consumer<EventMessage> eventDispatcher(
      final Channel channel,
      final ProtocolVersion version,
      final FrameEncoder.PayloadAllocator allocator) {
    return eventMessage ->
        flush(
            new FlushItem.Framed(
                channel, eventMessage.encode(version), null, allocator, f -> f.response.release()));
  }
}
