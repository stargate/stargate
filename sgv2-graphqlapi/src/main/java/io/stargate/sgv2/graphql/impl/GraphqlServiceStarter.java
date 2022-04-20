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
package io.stargate.sgv2.graphql.impl;

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

@Command(name = "sgv2-graphqlapi")
public class GraphqlServiceStarter {

  @Option(
      name = {"--disable-playground"},
      title = "disable-playground",
      description = "whether to disable the GraphQL Playground UI (exposed at /playground)")
  private boolean disablePlayground;

  @Option(
      name = {"--disable-default-keyspace"},
      title = "disable-playground",
      description =
          "whether to disable the \"default keyspace\" feature (which consists in falling back to "
              + "the oldest keyspace if not specified in the URL path)")
  private boolean disableDefaultKeyspace;

  public void start() throws Exception {
    final MetricsImpl metricsImpl = new MetricsImpl();
    final HttpMetricsTagProvider httpMetricsTags = new NoopHttpMetricsTagProvider();

    GraphqlServiceServer server =
        new GraphqlServiceServer(
            metricsImpl, metricsImpl, httpMetricsTags, disablePlayground, disableDefaultKeyspace);
    server.run("server", "config.yaml");
  }

  public static void main(String[] args) {
    cli(args, GraphqlServiceStarter.class);
  }

  protected static void printHelp(SingleCommand<?> c) {
    try {
      new CliCommandUsageGenerator(79)
          .usage(c.getCommandMetadata(), c.getParserConfiguration(), System.err);
    } catch (IOException e) {
      throw new IOError(e);
    }
  }

  protected static void cli(String[] args, Class<? extends GraphqlServiceStarter> starterClass) {
    SingleCommand<? extends GraphqlServiceStarter> parser =
        SingleCommand.singleCommand(starterClass);
    try {
      ParseResult<? extends GraphqlServiceStarter> result = parser.parseWithResult(args);

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
        // if a user just uses -h/--help
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
