/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console.cmd;

import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;

final class Clear implements CliSpecificCommand {

  private static final String HELP = "clear:" + System.lineSeparator()
      + "\tClear the current terminal.";

  private final Runnable screenCleaner;

  private Clear(final Runnable screenCleaner) {
    this.screenCleaner = Objects.requireNonNull(screenCleaner, "screenCleaner");
  }

  static Clear create(final Runnable screenCleaner) {
    return new Clear(screenCleaner);
  }

  @Override
  public String getName() {
    return "clear";
  }

  @Override
  public String getHelpMessage() {
    return HELP;
  }

  @Override
  public void execute(final List<String> args, final PrintWriter terminal) {
    CliCmdUtil.ensureArgCountBounds(args, 0, 0, HELP);
    screenCleaner.run();
  }
}
