package io.stargate.db;

import static java.lang.String.format;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.annotation.Nullable;

public class ClientInfo {

  public static final String PROXY_SOURCE_ADDRESS_HEADER = "proxy_source_address_header";
  public static final String PROXY_PUBLIC_ADDRESS_HEADER = "proxy_public_address_header";

  private final InetSocketAddress remoteAddress;
  private final @Nullable InetSocketAddress publicAddress;

  private volatile DriverInfo driverInfo;

  private AuthenticatedUser authenticatedUser;
  private Map<String, ByteBuffer> serializedAuthData;

  public ClientInfo(InetSocketAddress remoteAddress, @Nullable InetSocketAddress publicAddress) {
    this.remoteAddress = remoteAddress;
    this.publicAddress = publicAddress;
  }

  public InetSocketAddress remoteAddress() {
    return remoteAddress;
  }

  public Optional<InetSocketAddress> publicAddress() {
    return Optional.ofNullable(publicAddress);
  }

  public void registerDriverInfo(DriverInfo info) {
    if (driverInfo != null) {
      throw new IllegalStateException(
          format("Driver info has already been set (to %s)", driverInfo));
    }
    this.driverInfo = info;
  }

  public Optional<DriverInfo> driverInfo() {
    return Optional.ofNullable(driverInfo);
  }

  public AuthenticatedUser getAuthenticatedUser() {
    return authenticatedUser;
  }

  public void setAuthenticatedUser(AuthenticatedUser authenticatedUser) {
    this.authenticatedUser = authenticatedUser;
    this.serializedAuthData = AuthenticatedUser.Serializer.serialize(authenticatedUser);
  }

  public void storeAuthenticationData(Map<String, ByteBuffer> payload) {
    if (serializedAuthData != null) {
      for (Entry<String, ByteBuffer> e : serializedAuthData.entrySet()) {
        payload.put(e.getKey(), e.getValue().duplicate());
      }
    }
  }

  @Override
  public String toString() {
    return "ClientInfo{"
        + "remoteAddress="
        + remoteAddress
        + ", publicAddress="
        + publicAddress
        + ", driverInfo="
        + driverInfo
        + '}';
  }
}
