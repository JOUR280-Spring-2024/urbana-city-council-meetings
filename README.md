# Urbana's City Council Meetings

This scraper extracts data from Urbana's [City Council Meetings](https://urbana-il.municodemeetings.com/).

The data is stored as a SQLite database named `urbana_council_meetings.sqlite`.

We also extract the text from PDFs to make it searchable via SQL.

The code can be run every day to add new records to the database. Previous records are retained and new
records are added.

If you end up using this database in a news report, please give credit to Darryl Norwood.
