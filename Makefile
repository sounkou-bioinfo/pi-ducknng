.PHONY: readme check-readme persistent-r-proof

readme:
	quarto render README.qmd --to gfm

check-readme:
	@tmp=$$(mktemp); \
	cp README.md "$$tmp"; \
	quarto render README.qmd --to gfm >/dev/null; \
	diff -u "$$tmp" README.md; \
	status=$$?; \
	rm -f "$$tmp"; \
	exit $$status

persistent-r-proof:
	Rscript --vanilla tools/persistent-r-proof.R
