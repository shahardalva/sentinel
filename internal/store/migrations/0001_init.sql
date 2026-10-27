-- Tenants and their client keys. Only SHA-256 digests of keys are stored;
-- the plaintext is shown once, when the key is created.

CREATE TABLE tenants (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text        NOT NULL CONSTRAINT tenants_name_key UNIQUE,
    created_at  timestamptz NOT NULL DEFAULT now(),
    disabled_at timestamptz NULL
);

CREATE TABLE api_keys (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  uuid        NOT NULL CONSTRAINT api_keys_tenant_id_fkey REFERENCES tenants (id),
    prefix     text        NOT NULL,
    digest     bytea       NOT NULL CONSTRAINT api_keys_digest_key UNIQUE
                           CONSTRAINT api_keys_digest_length CHECK (octet_length(digest) = 32),
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz NULL
);

CREATE INDEX api_keys_tenant_id_idx ON api_keys (tenant_id);
