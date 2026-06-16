# cofiswarm-observer-sdk

Cofiswarm component: `observer-sdk`.

- Layout: [REPO-STANDARD-LAYOUT](https://github.com/keepdevops/cofiswarmdev/blob/main/docs/REPO-STANDARD-LAYOUT.md)
- Migration: [MIGRATION-SPRINTS](https://github.com/keepdevops/cofiswarmdev/blob/main/docs/MIGRATION-SPRINTS.md)

## FHS paths

| Path | Purpose |
|------|---------|
| `/etc/cofiswarm/observer-sdk/` | config |
| `/var/lib/cofiswarm/observer-sdk/` | state |
| `/var/log/cofiswarm/observer-sdk/` | logs |

## Test

```bash
./test/scripts/assert-layout.sh observer-sdk
```
