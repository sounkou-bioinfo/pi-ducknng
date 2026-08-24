.PHONY: readme check-readme

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
