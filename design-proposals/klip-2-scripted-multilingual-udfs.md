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

We therefore find ourselves in this peculiar situation. We initially lower the barrier entry into the streaming processing space by removing the Java prequisite, only to reintroduce the requirement in what is surely one of the most attractive features of KSQL.

By allowing UDFs to be written in multiple languages, we can:

- unlock this feature for non-Java developers
- increase UDF-related feature adoption

Furthermore, even Java developers may find the process of writing a simple UDF, e.g. `MULTIPLY`, a little tedious. Java-based UDFs require some level of ceremony to build and deploy. A project structure must be defined, a build system employed, and KSQL itself must be restarted to pick up the new JAR. Instead, if we allow multilingual UDFs to be defined as inline scripts, we can save the ceremony for truly complex UDFs that warrant the extra effort.

## What is in scope

- Extending the KSQL language to support inline, scripted, multilingual UDFs
- Hot-reloading of non-Java UDFs via `CREATE OR REPLACE` queries

## What is not in scope

- UDAFs
- Non-Java UDFs that cannot be expressed as an inline function (e.g. a larger program that would need to be loaded from disk)

The above items are too complicated for the initial implementation of this feature. We should start simple and build a solid foundation with inline, multilingual UDFs before attempting more complicated variations of this work.

## Value/Return

- Unlock UDFs for non-Java developers
- Rapid prototyping via hot-reloading scripted UDFs

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

_What tests do you plan to write?  What are the failure scenarios that we do / do not cover? It goes without saying that most classes should have unit tests. This section is more focussed on the integration and system tests that you need to write to test the changes you are making._

## Documentation Updates

_What changes need to be made to the documentation? For example_

* Do we need to change the KSQL quickstart(s) and/or the KSQL demo to showcase the new functionality? What are specific changes that should be made?
* Do we need to update the syntax reference?
* Do we need to add/update/remove examples?
* Do we need to update the FAQs?
* Do we need to add documentation so that users know how to configure KSQL to use the new feature? 
* Etc.

_This section should try to be as close as possible to the eventual documentation updates that need to me made, since that will force us into thinking how the feature is actually going to be used, and how users can be on-boarded onto the new feature. The upside is that the documentation will be ready to go before any work even begins, so only the fun part is left._

# Compatibility implications

_Will the proposed changes break existing queries or work flows?_

_Are we deprecating existing APIs with these changes? If so, when do we plan to remove the underlying code?_

_If we are removing old functionality, what is the migration plan?_

## Performance implications

_Will the proposed changes affect performance, (either positively or negatively)._
