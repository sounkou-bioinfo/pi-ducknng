DUCKNNG_CI_TOOLS_COMMIT := ef15a2a7453db5b4f85b7c668a545ae2f1193ff6
DUCKNNG_EXTENSION_VERSION := v0.1.3-duckdb1.5.4
DUCKNNG_EXTENSION := vendor/ducknng/build/release/ducknng.duckdb_extension
# Executable documentation runs the Pi version pinned in DEPENDENCIES, found
# on PATH so rendered commands read `pi` rather than a machine-local path.
DOCS_PATH := $(CURDIR)/node_modules/.bin:$(PATH)

.PHONY: readme check-readme vignettes check-vignettes site persistent-r-proof ducknng-extension check-pi check-r check

readme:
	env -u MAKEFLAGS -u MAKELEVEL -u MFLAGS PATH="$(DOCS_PATH)" npm run readme:qmd
	@$(MAKE) --no-print-directory check-readme

check-readme:
	@grep -q 'extension="./extensions/pi-ducknng/index.ts"' README.qmd
	@grep -q '^> AGENT_DUCKNNG_MANIFEST_CALL_OK' README.md
	@grep -q '^> AGENT_FANOUT_SENT' README.md
	@for worker in w-cyl w-gear w-am; do \
		grep -Eq "^ *AGENT_WORKER_REPLIED $$worker" README.md || \
		{ echo "missing receipt for $$worker"; exit 1; }; \
	done
	@grep -Eq '^ *COORDINATION_FANIN_VERIFIED$$' README.md
	@grep -q '^> AGENT_FANIN_DONE' README.md
	@grep -Eq '"agent_id":"reviewer","events.url":"wss://' README.md
	@grep -q 'unauthorized: tls:cn:intruder has no grant' README.md
	@! grep -qF '$(CURDIR)' README.md

# ONLY=name[,name] rebuilds a subset of the guides.
vignettes:
	PATH="$(DOCS_PATH)" Rscript --vanilla scripts/precompile-vignettes.R $(if $(ONLY),--only $(ONLY))

check-vignettes:
	Rscript --vanilla scripts/precompile-vignettes.R --check

site: check-vignettes
	Rscript --vanilla tools/build-site.R

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

check-pi:
	npm run check

check-r:
	Rscript --vanilla -e 'lib <- tempfile("piducknng-lib-"); dir.create(lib); install.packages(".", lib = lib, repos = NULL, type = "source", INSTALL_opts = "--no-test-load", quiet = TRUE); tinytest::test_package("piducknng", lib.loc = lib)'

check: check-readme check-vignettes check-pi check-r
