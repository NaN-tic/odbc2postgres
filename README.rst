odbc2postgres
=============

*odbc2postgres* is a simple python3 script that, run on a Windows machine, will
dump all data from an ODBC datasource (so far tested with SQL Server) into a
PostgreSQL server, probably located in remote host.

The script also creates a "odbc2postgres" table where it records the start and end
timestamps of the process. This way, you'll be able to monitor if the process
has finished just by accessing the destintation PostgreSQL database.
