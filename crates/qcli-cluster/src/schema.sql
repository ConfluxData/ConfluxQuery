CREATE TABLE IF NOT EXISTS qcli_cluster_schema (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    version integer NOT NULL
);
INSERT INTO qcli_cluster_schema(singleton, version) VALUES(true, 1)
ON CONFLICT(singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS qcli_nodes (
    node_id text PRIMARY KEY,
    instance_version text NOT NULL,
    capabilities text[] NOT NULL,
    draining boolean NOT NULL,
    lease_epoch bigint NOT NULL,
    lease_expires_at timestamptz NOT NULL
);
CREATE SEQUENCE IF NOT EXISTS qcli_fencing_token;
CREATE TABLE IF NOT EXISTS qcli_resources (
    kind text NOT NULL,
    resource_id text NOT NULL,
    principal_id text NOT NULL,
    version bigint NOT NULL,
    payload jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY(kind, resource_id)
);
CREATE TABLE IF NOT EXISTS qcli_query_leases (
    query_id text PRIMARY KEY,
    principal_id text NOT NULL,
    owner_node_id text NOT NULL,
    fencing_token bigint NOT NULL,
    lease_expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS qcli_quota_permits (
    permit_id text PRIMARY KEY,
    principal_id text NOT NULL,
    quota text NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS qcli_quota_lookup ON qcli_quota_permits(principal_id, quota, expires_at);
