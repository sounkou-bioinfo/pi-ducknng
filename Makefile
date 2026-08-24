.PHONY: readme check-readme persistent-r-proof check-pi check

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

check-pi:
	npm run check

check: check-readme check-pi
