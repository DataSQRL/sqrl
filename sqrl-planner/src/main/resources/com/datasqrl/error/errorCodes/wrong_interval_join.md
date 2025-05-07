An interval join is between two stream tables where the condition restricts
the timestamps of both tables, so the join can be executed efficiently.

If you did not intend for this join to be an interval join, declare it as
an `INNER JOIN` instead.