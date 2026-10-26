-- Who moved a ref: the subjects whose requests the move carried out. Empty for moves recorded
-- before this column, and for the server's own.
ALTER TABLE ref_log ADD COLUMN actor TEXT NOT NULL DEFAULT '';
