DUCKNNG_CI_TOOLS_COMMIT := ef15a2a7453db5b4f85b7c668a545ae2f1193ff6
DUCKNNG_EXTENSION_VERSION := v0.1.1-duckdb1.5.4
DUCKNNG_EXTENSION := vendor/ducknng/build/release/ducknng.duckdb_extension

.PHONY: readme check-readme persistent-r-proof ducknng-extension pi-proof check-pi check

readme:
	env -u MAKEFLAGS -u MAKELEVEL -u MFLAGS npm run readme:qmd

check-readme:
	@tmp=$$(mktemp); \
	cp README.md "$$tmp"; \
	env -u MAKEFLAGS -u MAKELEVEL -u MFLAGS npm run readme:qmd >/dev/null; \
	diff -u "$$tmp" README.md; \
	status=$$?; \
	rm -f "$$tmp"; \
	exit $$status

persistent-r-proof:
	Rscript --vanilla tools/persistent-r-proof.R

ducknng-extension:
	@if [ ! -d vendor/ducknng/extension-ci-tools/.git ]; then \
		rm -rf vendor/ducknng/extension-ci-tools; \
		git clone https://github.com/duckdb/extension-ci-tools vendor/ducknng/extension-ci-tools; \
	fi
	git -C vendor/ducknng/extension-ci-tools checkout --quiet $(DUCKNNG_CI_TOOLS_COMMIT)
	@mkdir -p vendor/ducknng/configure
	@printf '%s\n' '$(DUCKNNG_EXTENSION_VERSION)' > vendor/ducknng/configure/extension_version.txt
	$(MAKE) -C vendor/ducknng configure EXTENSION_VERSION=$(DUCKNNG_EXTENSION_VERSION)
	$(MAKE) -C vendor/ducknng release EXTENSION_VERSION=$(DUCKNNG_EXTENSION_VERSION) -j$$(nproc 2>/dev/null || echo 2)
	@test -f $(DUCKNNG_EXTENSION)

pi-proof:
	node scripts/pi-rpc-proof.mjs

check-pi:
	npm run check

check: check-readme check-pi
