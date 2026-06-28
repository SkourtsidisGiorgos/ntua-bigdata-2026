#!/usr/bin/env bash
# Run the local JUnit tests under the project's JDK 17 (system Java is too new
# for Spark). Loader tests that need ./data are auto-skipped when it's absent.
#
#   ./scripts/test.sh            # all tests
#   ./scripts/test.sh -Dtest=Q1Test   # a single test class
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/env.sh   # JAVA_HOME=17
mvn test "$@"
