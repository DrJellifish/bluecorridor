# Blue Corridor — Mediterranean Surface Drift Ops
.PHONY: all fetch stitch sim post publish

all: fetch stitch sim post publish

fetch:
	python -m src.fetch.pull_cmems
	python -m src.fetch.pull_ecmwf_gfs

stitch:
	python -m src.fetch.stitch_fields

sim:
	python -m src.model.run_opendrift

post:
	python -m src.post.summarize_runs
	python -m src.post.make_geojson

publish:
	mkdir -p site/data/latest
	cp -f data/outputs/*.geojson site/data/latest/ || true
	cp -f data/outputs/*.csv site/data/latest/ || true
	@echo "Publish complete → site/data/latest/"
