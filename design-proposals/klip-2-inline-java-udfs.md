# KLIP 3 - Support inline JAVA UDFs

**Author**: [Mitch Seymour][mitch-seymour]

**Release target**: 5.3

**Status**: In Discussion

<!-- TODO: replace with link to PR -->
**Discussion**: link

[mitch-seymour]: https://github.com/mitch-seymour

## tl;dr

Allowing users to create inline Java UDFs will:
- Reduce the amount of code and time that is needed to deploy simple UDFs
- Enable rapid prototyping of new data transformation logic
- Strengthen feature parity between KSQL and certain RDBMSes (e.g. Postgres, which supports [inline UDFs][postgres])

[postgres]: https://www.postgresql.org/docs/current/plpython-funcs.html

## Motivation and background

The current process of building and deploying UDFs works well for functions that require third-party dependencies, leverage multiple classes, or for business logic that cannot be expressed in a few lines of code. For simpler UDFs, however, the current build and deployment process feels a little tedious.


To illustrate this point, consider the following example. Let's create a UDF named `MULTIPLY` that multiplies 2 numbers.

__Current approach:__

- Write a Java class that implements the business logic of our UDF (multiply two numbers)
- Add the `@UdfDescription` and `@Udf` annotations to our class
- Create a build file (e.g. `pom.xml` or `build.gradle`) for building our project
- Package the artifact into an uber JAR
- Copy the uber JAR to the `ext/` directory (`ksql.extension.dir`)
- Restart KSQL server instances to pick up the new UDF

__New approach:__

As you can see above, implementing the business logic of our UDF is only the first of many steps using the current approach. For simple UDFs (i.e. UDFs that don't require third-party dependencies, and can be expressed in a few lines of code), it would be much easier if we could just worry about implementation details of our function, and not the build / deployment process as well. For example, using the new `CREATE OR REPLACE` query, we could create the `MULTIPLY` UDF as follows:

```sql
CREATE OR REPLACE FUNCTION MULTIPLY(X INT, Y INT)
RETURNS INT
LANGUAGE JAVA
WITH (author='mitch', description='multiply 2 numbers', version='0.1.0')
AS $$
    return X * Y ;
$$;
```

The above query would automatically update the internal function registry as needed so that we can avoid restarting any KSQL servers to pick up the new UDF. It also requires a lot less code than the original implementation. Less code, combined with a quicker deployment model, means a better development experience for UDF creators.

## What is in scope

- Extending the KSQL language to support the creation of inline, Java UDFs
- Extending the KSQL language to support to deletion of inline, Java UDFs

## What is not in scope

- UDAFs
- UDFs that cannot be expressed as an inline function (e.g. a larger program that would need to be loaded from disk)
- UDFs written in languages other than Java

The above items are too complicated for the initial implementation of this feature. We should start simple and build a solid foundation with the current proposal before attempting more complicated variations of this work.

## Value/Return

- Write less code for implementing simple, custom functions
- Quick deployment model
- Enables rapid prototyping / experimentation of UDF logic

## Public APIS

### KSQL language

#### CREATE [OR REPLACE] FUNCTION

The KSQL language will be extended with the following query for creating inline, multilingual UDFs:

```sql
CREATE (OR REPLACE?) FUNCTION function_name ( { field_name data_type } [, ...] )
RETURNS data_type
LANGUAGE language_name
WITH ( property_name = expression [, ...] );
AS $$
    inline_script
$$;
```

Example:

Here's an example UDF that coverts HTTP response codes (500, 404, [418][i_am_a_teapot]), to a status major string (`5xx`, `4xx`, etc).

[i_am_a_teapot]: https://httpstatuses.com/418

```sql
CREATE OR REPLACE FUNCTION STATUS_MAJOR(status_code INT) 
RETURNS VARCHAR
LANGUAGE JAVA
WITH (author='Mitch Seymour', description='...', version='0.1.0')
AS $$
  return String.valueOf(STATUS_CODE).charAt(0) + "xx";
$$;
```

Functions created using this new query will be discoverable via `SHOW FUNCTIONS` and can be described using the `DESCRIBE FUNCTION` query. We may want to impose a max identifier length for the function name (e.g. 60 characters).

#### DROP FUNCTION [IF EXISTS]
We will also extend KSQL with a query for dropping UDFs that were created using the new `CREATE FUNCTION` query.

```sql
DROP FUNCTION (IF EXISTS?) function_name
```

If a user tries to drop a UDF that was not created via the `CREATE FUNCTION` query, they will receive an error.

### Configurations
N/A

## Design
TODO

## Test plan

Tests will cover the following:

- Query parsing / translation
- New methods added to the internal function registry for updating / dropping scripted UDFs via `CREATE OR REPLACE FUNCTION` and `DROP FUNCTION` queries
- Any other changes that are made to existing classes
- Mocked failure scenarios will include scripts that aren't executable (e.g. because of syntax errors) and `ClassCastException` / `NullPointerException` when the script doesn't return the expected value


## Documentation Updates
- The `Implement a Custom Function` section will need to be updated in the [KSQL Function Reference](/docs/developer-guide/udf.rst) to include instructions for implementing non-Java UDFs. Any reference to `UDFs` in the current documentation will be updated to `Java UDFs`.

- The [Syntax Reference](/docs/developer-guide/syntax-reference.rst) will need to be updated to include the new commands with query examples

# Compatibility implications
N/A

## Performance implications
N/A
