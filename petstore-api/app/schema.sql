-- Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
-- SPDX-License-Identifier: MIT-0
--
-- Pet store schema for Aurora DSQL.
--
-- DSQL compatibility notes:
--   * Primary key is a client-generated UUID. DSQL has no SERIAL / sequences,
--     so the application supplies the id (see app/db.py).
--   * No foreign keys -- DSQL does not support them. Relationships, if added
--     later (owners, vets), are enforced in the application layer.
--   * write_region and updated_at capture which Region a row was written
--     through and when, which the observability / switchover demo visualizes.

CREATE TABLE IF NOT EXISTS pets (
    pet_id       UUID PRIMARY KEY,
    pet_type     VARCHAR(64)  NOT NULL,
    pet_name     VARCHAR(128) NOT NULL,
    pet_age      INTEGER      NOT NULL DEFAULT 0,
    write_region VARCHAR(32)  NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL
);
