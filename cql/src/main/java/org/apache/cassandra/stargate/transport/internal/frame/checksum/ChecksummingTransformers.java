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
package org.apache.cassandra.stargate.transport.internal.frame.checksum;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.cassandra.stargate.transport.internal.CqlServer;
import org.apache.cassandra.stargate.transport.internal.TransportDescriptor;
import org.apache.cassandra.stargate.transport.internal.frame.compress.Compressor;
import org.apache.cassandra.stargate.transport.internal.frame.compress.LZ4Compressor;
import org.apache.cassandra.stargate.transport.internal.frame.compress.SnappyCompressor;
import org.apache.cassandra.utils.ChecksumType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds the {@link ChecksummingTransformer} instances for a given {@link CqlServer} instance. */
public class ChecksummingTransformers {

  private static final Logger logger = LoggerFactory.getLogger(ChecksummingTransformers.class);

  private final Map<ChecksumType, Map<Compressor, ChecksummingTransformer>> transformers;

  public ChecksummingTransformers(TransportDescriptor transportDescriptor) {
    this.transformers = buildTransformers(transportDescriptor);
  }

  public ChecksummingTransformer get(ChecksumType checksumType, Compressor compressor) {
    ChecksummingTransformer transformer = transformers.get(checksumType).get(compressor);

    if (transformer == null) {
      logger.warn(
          "Invalid compression/checksum options supplied. {} / {}",
          checksumType,
          compressor == null ? "none" : compressor.getClass().getName());
      throw new RuntimeException("Invalid compression / checksum options supplied");
    }

    return transformer;
  }

  private Map<ChecksumType, Map<Compressor, ChecksummingTransformer>> buildTransformers(
      TransportDescriptor transportDescriptor) {
    int blockSize = transportDescriptor.getNativeTransportFrameBlockSize();

    EnumMap<ChecksumType, Map<Compressor, ChecksummingTransformer>> transformers =
        new EnumMap<>(ChecksumType.class);

    Map<Compressor, ChecksummingTransformer> crc32Compressors = new HashMap<>();
    crc32Compressors.put(null, new ChecksummingTransformer(ChecksumType.CRC32, blockSize, null));
    crc32Compressors.put(
        LZ4Compressor.INSTANCE,
        new ChecksummingTransformer(ChecksumType.CRC32, blockSize, LZ4Compressor.INSTANCE));
    crc32Compressors.put(
        SnappyCompressor.INSTANCE,
        new ChecksummingTransformer(ChecksumType.CRC32, blockSize, SnappyCompressor.INSTANCE));
    transformers.put(ChecksumType.CRC32, Collections.unmodifiableMap(crc32Compressors));

    Map<Compressor, ChecksummingTransformer> adler32Compressors = new HashMap<>();
    adler32Compressors.put(
        null, new ChecksummingTransformer(ChecksumType.ADLER32, blockSize, null));
    adler32Compressors.put(
        LZ4Compressor.INSTANCE,
        new ChecksummingTransformer(ChecksumType.ADLER32, blockSize, LZ4Compressor.INSTANCE));
    adler32Compressors.put(
        SnappyCompressor.INSTANCE,
        new ChecksummingTransformer(ChecksumType.ADLER32, blockSize, SnappyCompressor.INSTANCE));
    transformers.put(ChecksumType.ADLER32, Collections.unmodifiableMap(adler32Compressors));

    return Collections.unmodifiableMap(transformers);
  }
}
