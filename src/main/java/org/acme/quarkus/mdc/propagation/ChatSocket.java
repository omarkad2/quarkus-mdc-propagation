package org.acme.quarkus.mdc.propagation;

import io.quarkus.logging.Log;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import org.slf4j.MDC;

import javax.enterprise.context.ApplicationScoped;
import javax.websocket.*;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.time.Instant;

@ServerEndpoint("/chat/{username}")
@ApplicationScoped
public class ChatSocket {
  private final ReactiveValueCommands<String, Long> valueCommands;

  public ChatSocket(ReactiveRedisDataSource reactiveRedisDataSource) {
    this.valueCommands = reactiveRedisDataSource.value(Long.class);
  }

  @OnOpen
  public void onOpen(Session session, @PathParam("username") String username) {
    storeLatestHeartbeat(username, Instant.now().toEpochMilli());
  }

  @OnClose
  public void onClose(Session session, @PathParam("username") String username) {
  }

  @OnError
  public void onError(Session session, @PathParam("username") String username, Throwable throwable) {
    Log.error("onError", throwable);
  }

  @OnMessage
  public void onMessage(String message, @PathParam("username") String username) {
    storeLatestHeartbeat(username, Instant.now().toEpochMilli());
  }

  private void storeLatestHeartbeat(String username, long latestHeartbeat) {
    MDC.put("user", username);
    Log.infof("start");
    valueCommands.set(username, latestHeartbeat)
        // Log missing MDC context
        .invoke(() -> Log.infof("New heartbeat: %s", latestHeartbeat))
        .subscribe().with(
            (__) -> {
              Log.info("end");
              MDC.clear();
            });
  }
}
