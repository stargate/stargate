/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.dynamosvc.impl;

import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.help.cli.CliCommandUsageGenerator;
import com.github.rvesse.airline.parser.ParseResult;
import com.github.rvesse.airline.parser.errors.ParseException;
import io.stargate.core.metrics.api.HttpMetricsTagProvider;
import io.stargate.core.metrics.api.NoopHttpMetricsTagProvider;
import io.stargate.core.metrics.impl.MetricsImpl;
import java.io.IOError;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Command(name = "sgv2-dynamo-service")
public class DynamoServiceStarter {
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Order {
    int value();
  }

  // Configuration option placeholder: not in use
  @Order(value = 1)
  @Option(
      name = {"--listen"},
      title = "listen_ip",
      arity = 1,
      description = "address this service should listen on (default: 127.0.0.1)")
  protected String listenHostStr = "127.0.0.1";

  private HttpMetricsTagProvider httpMetricsTagProvider;

  /*
  public void setHttpMetricsTagProvider(HttpMetricsTagProvider httpMetricsTagProvider) {
    this.httpMetricsTagProvider = httpMetricsTagProvider;
  }
   */

  public void start() throws Exception {
    // Note: now we have all configuration settings configured on "this"; should
    // pass whatever we want/need

    // Then Metrics-related things: hard-coded initially
    final MetricsImpl metricsImpl = new MetricsImpl();
    final HttpMetricsTagProvider httpMetricsTags = new NoopHttpMetricsTagProvider();

    DynamoServiceServer server = new DynamoServiceServer(metricsImpl, metricsImpl, httpMetricsTags);
    server.run("server", "config.yaml");
  }

  public static void main(String[] args) throws Exception {
    cli(args, DynamoServiceStarter.class);
  }

  /** Prints help with options in the order specified in {@link Order} */
  protected static void printHelp(SingleCommand c) {
    try {
      new CliCommandUsageGenerator(
              79,
              (a, b) -> {
                Order aO = a.getAccessors().iterator().next().getAnnotation(Order.class);
                Order bO = b.getAccessors().iterator().next().getAnnotation(Order.class);

                if (aO == null && bO == null) return 0;

                if (aO == null) return 1;

                if (bO == null) return -1;

                return aO.value() - bO.value();
              },
              false)
          .usage(c.getCommandMetadata(), c.getParserConfiguration(), System.err);
    } catch (IOException e) {
      throw new IOError(e);
    }
  }

  protected static void cli(String[] args, Class<? extends DynamoServiceStarter> starterClass) {
    SingleCommand<? extends DynamoServiceStarter> parser =
        SingleCommand.singleCommand(starterClass);
    try {
      ParseResult<? extends DynamoServiceStarter> result = parser.parseWithResult(args);

      if (result.wasSuccessful()) {
        // Parsed successfully, so just run the command and exit
        result.getCommand().start();
      } else {

        printHelp(parser);
        System.err.println();

        // Parsing failed
        // Display errors and then the help information
        System.err.printf("%d errors encountered:%n", result.getErrors().size());
        int i = 1;
        for (ParseException e : result.getErrors()) {
          System.err.printf("Error %d: %s%n", i, e.getMessage());
          i++;
        }

        System.err.println();
      }
    } catch (ParseException p) {
      printHelp(parser);

      // noinspection StatementWithEmptyBody
      if (args.length > 0 && (args[0].equals("-h") || args[0].equals("--help"))) {
        // don't tell the user it's a usage error
        // a bug in airline (https://github.com/airlift/airline/issues/44) prints a usage error even
        // if
        // a user just uses -h/--help
      } else {
        System.err.println();
        System.err.printf("Usage error: %s%n", p.getMessage());
        System.err.println();
      }
    } catch (Exception e) {
      // Errors should be being collected so if anything is thrown it is unexpected
      System.err.printf("Unexpected error: %s%n", e.getMessage());
      e.printStackTrace(System.err);

      System.exit(1);
    }
  }
}
