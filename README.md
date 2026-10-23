# multi-tour-workflow-onboard-tenant

> Workflow saga onboard-tenant (identity-audit), with compensations

Part of the **Multi Tour** distributed system — team `multi-tour`, Grupo 2.
Governance and documentation live in [`multi-tour-docs`](https://github.com/code-corhuila/multi-tour-docs).

## What it is

The saga that onboards a tenant (ADR-004 D3): it creates the pending tenant in `identity-audit`, the first Administrator's account in `multi-tour-security`, and records the Administrator and the initial status, with a compensation for each step. Its state is persisted after every step.

## How to start it

Run the migrator once per environment (see `deploy/compose.yml`), then `docker compose -f deploy/compose.yml up workflow-onboard-tenant`, with the variables of `.env.example` (the compose file maps the secrets from `env/<environment>.env` of `multi-tour-infra-postgres`) and the `SERVICE_TOKEN` issued by the identity service. The service listens on 8080 inside the platform network and publishes no port. Entry through the gateway: `POST /api/v1/sagas/onboard-tenant` and `GET /api/v1/sagas/onboard-tenant/{id}` (see `openapi/openapi.yaml`).

## Where the data is

In PostgreSQL, schema `workflow_onboard_tenant` of the single instance (ADR-005 E4); its migrations will live in `db/` of this repository. Nothing is stored yet.

## How to test it

`gofmt -l .` must print nothing, then `go vet ./...` and `go test ./...` (Go 1.26). The same three run in `ci.yml` on pull requests to `develop` and `qa`, with a PostgreSQL service for `TEST_DATABASE_URL`.

## What is missing

Declared debts: the audit actor of the participant calls is the service, because the participant contracts have no field for the person. Not verified end to end against the real participants yet.

## Branching

Three permanent branches. **None of them accepts a direct commit** — you enter through a child
branch and leave through a Pull Request.

```
develop  <--PR--  feat/... fix/... chore/...
qa       <--PR--  qa/...
main     <--PR--  release/...  hotfix/...
```

Promotion happens **by re-application** (`git cherry-pick -x`), never by merging one permanent
branch into another: `merge develop -> qa` and `merge qa -> main` do not exist in this model.

`main` requires **1 approval from `ariel5253`**. On `develop` and `qa` the team sets its own review
rule.

Full policy: `00-governance/branching-policy.md` in `multi-tour-docs`.
