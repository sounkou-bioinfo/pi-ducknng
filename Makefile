DUCKNNG_CI_TOOLS_COMMIT := ef15a2a7453db5b4f85b7c668a545ae2f1193ff6
DUCKNNG_EXTENSION_VERSION := v0.1.2-duckdb1.5.4
DUCKNNG_EXTENSION := vendor/ducknng/build/release/ducknng.duckdb_extension
MERMAID_CLI_VERSION := 11.12.0
MERMAID_PUPPETEER_ARGS ?=
# Executable documentation runs the Pi version pinned in DEPENDENCIES.
PIKNIT_PI := $(CURDIR)/node_modules/.bin/pi

.PHONY: architecture readme check-readme vignettes check-vignettes site persistent-r-proof ducknng-extension check-pi check-r check

architecture:
	npx --yes -p @mermaid-js/mermaid-cli@$(MERMAID_CLI_VERSION) mmdc \
		$(MERMAID_PUPPETEER_ARGS) \
		--input man/figures/architecture.mmd \
		--output man/figures/architecture.svg \
		--backgroundColor transparent

readme:
	env -u MAKEFLAGS -u MAKELEVEL -u MFLAGS PIKNIT_PI=$(PIKNIT_PI) npm run readme:qmd
	@$(MAKE) --no-print-directory check-readme

check-readme:
	@grep -q 'extension="./extensions/pi-ducknng/index.ts"' README.qmd
	@grep -q '^> AGENT_DUCKNNG_MANIFEST_CALL_OK' README.md
	@grep -q '^> AGENT_COORDINATION_SENT' README.md
	@grep -q '^> AGENT_COORDINATION_REPLIED' README.md
	@grep -Eq '^ *COORDINATION_ROUND_TRIP_VERIFIED$$' README.md
	@grep -q 'man/figures/architecture.svg' README.qmd README.md
	@test -s man/figures/architecture.mmd
	@test -s man/figures/architecture.svg
	@! grep -q '^``` mermaid' README.md

vignettes:
	PIKNIT_PI=$(PIKNIT_PI) Rscript --vanilla scripts/precompile-vignettes.R

check-vignettes:
	PIKNIT_PI=$(PIKNIT_PI) Rscript --vanilla scripts/precompile-vignettes.R --check

site: check-vignettes
	Rscript --vanilla -e 'pkgdown::build_site()'

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
