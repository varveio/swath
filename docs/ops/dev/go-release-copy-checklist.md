# Go release copy checklist

The README and homepage source describe **Go on main, unreleased**, with explicit
build-from-source instructions. The latest published distribution is Java v0.3.2.
This checklist stages repository metadata for the Go release; it does not authorize
merging, deploying, publishing a release or removing Java.

## GitHub description and topics

At the Go release, use this description:

> Parallel listing and inventory for large object stores and filesystems.

Topics:

```text
storage inventory cli filesystem s3 gcs azure-blob parquet disk-usage du go seekparquet
```

Preserve other relevant topics, such as `aws`, `object-storage` and `data-engineering`.
Remove `java` as a primary topic when the Java code is retired. Keep historical Java
release and documentation links. Do not advertise a released multi-backend download
while GitHub's latest release is still the Java distribution.

## Publication checklist

- [ ] Publish the Go release through the separately reviewed release process and remove
      Java as planned. Keep historical Java sources and docs reachable at release tags.
- [ ] Replace build-only instructions with the verified Go installation path. Verify
      binaries and containers before describing them as Go distributions.
- [ ] Update the GitHub description and topics above in the same release window.
- [ ] Update README, site notices, navigation and search/social metadata together.
      Retire the unreleased notices only after the matching installation path exists.
- [ ] Update `site/static/data/channel.json`, the home page's current-version line
      (`site/index.md`), the image tags in `docs/install.md`, the changelog and the site
      checks together.
- [ ] Recheck commands, backend limitations, output modes and integration schema support
      against the release. Keep `show` a summary command, not a query-engine claim.
- [ ] Keep measured count-only and sorted-Parquet timings separate, with revision, date,
      backend, source, machine, regions, workers and output mode. Add other-backend claims
      only with their own measurements.
- [ ] Check social images and old video captions. The existing Java recordings are
      historical evidence and must not appear to demonstrate Go.
- [ ] Run the site build/checks and required repository checks before merging. Merging
      site changes to `main` deploys the website automatically.
