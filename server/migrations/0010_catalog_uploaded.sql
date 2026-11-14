-- Metadata alone does not prove that bytes were uploaded. Existing records also
-- begin unacknowledged; verified objects can be acknowledged through the model.
ALTER TABLE catalog
    ADD COLUMN uploaded BOOLEAN NOT NULL DEFAULT FALSE AFTER format;
