# KLIP 2 - Support scripted, multilingual UDFs

**Author**: [Mitch Seymour][mitch-seymour]

**Release target**: 5.3

**Status**: In Discussion

<!-- TODO: replace with link to PR -->
**Discussion**: link

[mitch-seymour]: https://github.com/mitch-seymour

## tl;dr

The ability to write UDFs in languages other than Java (e.g. Python, Ruby, Javascript) will allow more users to take advantage of UDFs

_The summary of WHY we we should do this, and (if possible) WHO benefits from this.  If this unclear or too verbose, it is a strong indication that we need to take a step back and re-think the proposal._

_Example: "Rebalancing enables elasticity and fault-tolerance of Streams applications. However, as of today rebalancing is a costly operation that we need to optimize to achieve faster and more efficient application starts/restarts/shutdowns, failovers, elasticity."._

## Motivation and background

KSQL allows developers to leverage the power of Kafka Streams without knowing Java. However, non-Java developers who use KSQL may quickly find themselves locked out of one of the most powerful features of KSQL: the ability to write custom functions for processing data.

We therefore find ourselves in a peculiar situation. We initially lower the barrier entry into the streaming processing space by removing the Java prequisite, only to reintroduce the requirement later.

Furthermore, even Java developers may find the process of writing a simple UDF, e.g. `MULTIPLY`, a little tedious. Java-based UDFs require some level of ceremony to build and deploy. A project structure must be defined, a build system employed, and KSQL itself must be restarted to pick up the new JAR. This seems overkill for some use cases, especially when the UDF logic can be expressed in a small number of lines of code.

By allowing UDFs to be written in multiple languages, we can:

- unlock this feature for non-Java developers, which will likely increase UDF-related feature adoption
- write less code for implementing simple, custom functions
- enable hot-reloading of UDF logic since a scripted, non-Java UDF doesn't need to be on the classpath when the server is started


## What is in scope

- Extending the KSQL language to support inline, scripted, multilingual UDFs
- Hot-reloading of non-Java UDFs via `CREATE OR REPLACE` queries

## What is not in scope

- UDAFs
- Non-Java UDFs that cannot be expressed as an inline function (e.g. a larger program that would need to be loaded from disk)

The above items are too complicated for the initial implementation of this feature. We should start simple and build a solid foundation with the current proposal before attempting more complicated variations of this work.

## Value/Return

- Unlock UDFs for non-Java developers
- Rapid prototyping via hot-reloading scripted UDFs
- Write less code

## Public APIS

### KSQL language

The KSQL language will be extended with the following queries:
- `CREATE OR REPLACE FUNCTION`
- `DROP FUNCTION` (will error if the function is not an inline script)

Here's a more complete example of the first query type:

```sql
CREATE OR REPLACE FUNCTION STATUS_MAJOR(status_code INT) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT AS $$
(code) => code.toString().charAt(0) + 'xx'
$$ 
WITH (author='Mitch Seymour', description='js udf example', version='0.1.0');
```

### Configurations
- A new configuration to enable experimental features. This reason this should be considered experimental is because our solution relies on GraalVM (see Design(#Design)), which is awaiting a 1.0 release (release candidate versions are available) 

## Design

[GraalVM Community Edition][gce] is a virtual machine that supports polyglot programming. It is drop-in replacement for Java 8 and soon to be Java 11. Users who would like to write UDFs in languages other than Java will be expected to run KSQL on GraalVM (instead of the HotSpot VM).

Regardless of which VM users run KSQL on, the GraalVM SDK will be added as a dependency to the `ksql-engine` and `ksql-parser` subprojects. When a user invokes the `CREATE OR REPLACE FUNCTION` query, the SDK will be used to create a [polyglot context][pg] that will determine whether or not the provided function is executable. For example, if a user were to invoke the following query while running KSQL on the HotSpot VM:

```
CREATE OR REPLACE FUNCTION STATUS_MAJOR(status_code INT) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT AS $$
(code) => code.toString().charAt(0) + 'xx'
$$ 
```

They will receive an error along the lines of:

```
Function is not executable. GraalVM is required for multilingual UDFs
```

Similarly, if KSQL is running on GraalVM but the inline script is not a valid lambda / executable, e.g.

```
CREATE OR REPLACE FUNCTION STATUS_MAJOR(status_code INT) 
RETURNS VARCHAR
LANGUAGE JAVASCRIPT AS $$
"hello"
$$ 
```

Then an error will be returned:

```
Function is not executable. Inline scripts must be defined as a lambda and must not contain syntax errors.
```

The GraalVM SDK handles this kind of validation for us, we just need to simply leverage the appropriate methods (e.g. [Value::canExecute][can_execute]

[can_execute]: https://www.graalvm.org/sdk/javadoc/org/graalvm/polyglot/Value.html#canExecute--
[gce]: graalvm.org
[pg]: https://www.graalvm.org/sdk/javadoc/org/graalvm/polyglot/Context.html

In order to support the hot-reloading feature, we will need to add a new method to the [MutableFunctionRegistry][mutable_fn_registry]: `addOrReplace`. This will only be invoked when leveraging the new `CREATE OR REPLACE` query. 

[mutable_fn_registry]: https://github.com/confluentinc/ksql/blob/5.2.x/ksql-common/src/main/java/io/confluent/ksql/function/MutableFunctionRegistry.java

Furthermore, a `dropInlineFunction` method will beed to be added to the [MutableFunctionRegistry][mutable_fn_registry] in order to drop any function that was created via the new `CREATE OR REPLACE` query. This method will be invoked whenever a `DROP FUNCTION` query is executed.

## Test plan

Tests will cover the following:

- Query parsing / translation
- New methods added to the internal function registry for updating / dropping scripted UDFs via `CREATE OR REPLACE FUNCTION` and `DROP FUNCTION` queries
- Any other changes that are made to existing classes
- Mocked failure scenarios will include scripts that aren't executable (e.g. because of syntax errors, guest language isn't installed, etc), and `ClassCastException` / `NullPointerException` when the script doesn't return the expected value


## Documentation Updates
- The `Implement a Custom Function` section will need to be updated in the [KSQL Function Reference](docs/developer-guide/udf.rst) to include instructions for implementing non-Java UDFs.

- The [Syntax Reference](docs/developer-guide/syntax-reference.rst) will need to be updated to include the new commands:
    - `CREATE OR REPLACE FUNCTION ...`
    - `DROP FUNCTION`

# Compatibility implications

- Users running KSQL on the HotSpot VM will _not_ be impacted by the feature at all. Using the new queries will result in errors indicating that the user must run KSQL on GraalVM if they wish to implement a non-Java UDF. However, all other queries will continue to work as expected.
- Users running KSQL on GraalVM will be able to execute the new queries. Existing queries and functionality will still be available, as well.

## Performance implications

The code changes themselves do not have any performance implications. However, running KSQL on top of GraalVM may offer some performance advantages as mentioned [here][graalvm-perf].

[graalvm-perf]: https://www.graalvm.org/docs/why-graal/#for-java-programs
