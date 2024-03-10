# Export a TypeDB database into CSV

This is a Python script to export all data from a TypeDB database to a CSV format.
Server address and the database name is set on the source code by the constants at the very beginning.
CSV files are written into a new folder with the name of the database exported.

## Output format

CSV files.
Entities are exported in the `entities` subfolder, one file per entity type.
Every entity is a single row.
The first column is `IID`, all the rest - owned attribute types.
Multivalued attributes are semicolon separated.
String attributes are in double quotes.

## Hot to use

1. Use `SERVER_ADDR` and `DATABASE_NAME` constants in the source code to set the address of TypeDB server
and the name of a database to export.
2. Run the Python script.
3. Find exported data in the folder named after the database.

## Warnings

This is an experimental test, rather than production ready utility.
Use AS IS, at your own discretion.
I assume no responsibility for any potential data corruption.
This script should not produce any changes to the exported database or any other database.
No guarantees.

## Known issues

1. Almost all logging is done at the Debug level.
2. The implementation is very far from being perfomant. Here are some ideas on how to improve performance:
   1. Use Queries (Fetch or Get) to retrieve data instances.
   2. Batch reads (use limit and offset with queries).
   3. Optimise the structure and logic. The first version uses for cycles and unoptimised for performance.
