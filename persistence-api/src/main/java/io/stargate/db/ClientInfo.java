package io.stargate.db;

import static java.lang.String.format;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nullable;

public class ClientInfo {

  private final InetSocketAddress remoteAddress;
  private final @Nullable InetSocketAddress publicAddress;

  private volatile DriverInfo driverInfo;

  private AuthenticatedUser authenticatedUser;
  private ByteBuffer token;
  private ByteBuffer roleName;
  private ByteBuffer isFromExternalAuth;

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
    this.token =
        authenticatedUser.token() != null
            ? ByteBuffer.wrap(authenticatedUser.token().getBytes(StandardCharsets.UTF_8))
            : null;
    this.roleName = ByteBuffer.wrap(authenticatedUser.name().getBytes(StandardCharsets.UTF_8));
    this.isFromExternalAuth =
        authenticatedUser.isFromExternalAuth() ? ByteBuffer.allocate(1) : null;
  }

  public ByteBuffer getToken() {
    return token;
  }

  public ByteBuffer getRoleName() {
    return roleName;
  }

  public ByteBuffer getIsFromExternalAuth() {
    return isFromExternalAuth;
  }

  @Override
  public String toString() {
    String base =
        publicAddress == null
            ? remoteAddress.toString()
            : format("%s[%s]", publicAddress, remoteAddress);

    return driverInfo == null ? base : format("%s (%s)", base, driverInfo);
  }
}
