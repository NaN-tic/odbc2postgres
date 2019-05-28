odbc2postgres
=============

*odbc2postgres* is a simple python3 script that, run on a Windows machine, will
dump all data from an ODBC datasource (so far tested with SQL Server) into a
PostgreSQL server, probably located in remote host.

The script also creates a "odbc2postgres" table where it records the start and
end timestamps of the process. This way, you'll be able to monitor if the
process has finished just by accessing the destintation PostgreSQL database.

Installation
------------

You should just use Python3 installer, ensuring it installs pip (the default).

If you don't have install priviledges in the Windows machine you may
alternatively use Anaconda:

https://www.anaconda.com

Once installed, go to the Windows menu and open "Anaconda Prompt"

Anaconda already has "pyodbc" so you just have to run "pip install psycopg2"

Then, you can execute using "python odbc2postgres.py"
