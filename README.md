# Crawchet

A simple package for scraping and processing crochet patterns.

## Description

Crawchet is not intended to be a standalone package, at least not for now. 
It currently exists solely to gather enough data for a Proof-of-Concept project under development ([Imgumi]()).
As such, expect frequent, large, breaking changes as the main project's requirements evolve. 

## Getting Started

### Prerequisites 

* TODO

### Installation

* TODO

## Usage
To gather and build the datasets, run the following commands in order from the `scripts/` directory

1. `python gascrape.py` - scrapes greatamigurumi and saves stores in `greatamigurumi.json`
2. `python write_urls.py` - extracts URLs from `greatamigurumi.json`, searches for archive.org matches, and outputs URL lists to .txt files
3. `python asyncget.py` - asynchronously fetches URL files from step 2 (`url_list.txt`, `archive_url_list.txt`) and writes contents to `merged.warc.gz`
4. `python build_datasets.py [args]` - parses `merged.warc.gz` and combines with `greatamigurumi.json` to make `df_master.pkl`. Simplifies html with readabilipy, writes `simphtml.json`
5. `python dl_images.py` - asynchronously fetches and downloads images extracted from `greatamigurumi.json` and `simphtml.json`


## Roadmap

### Near-term (PoC Phase)
- [ ] Add requirements.txt/environment.yaml
- [ ] Combine scripts and add flags for control flow
- [ ] Reduce dependence on GreatAmigurumi
  - [ ] Decouple pattern ids (ptid) from GA's post count
  - [ ] Allow archive.org search without blog post date
  - [ ] Replace language tags with language-id output (fastText/HF model)
- [ ] Handling Images in Patterns
  - [ ] Watermark detection (HF-Laion)
  - [ ] Classification (final-result, step-image, unrelated-image)
- [ ] Clean and Upload the PoC dataset to Kaggle (or similar)

### Long-term (Post-PoC Phase)
- [ ] Remove all special scraping/processing for GA, treat as generic data source
- [ ] Find better ways to source pattern-blog URLs
- [ ] Switch to spidering entire websites rather than a single endpoint extraction
- [ ] Multi-Medium Pattern Parsing
    - [ ] PDFs (ravelry)
    - [ ] Videos (YouTube transcripts)
    - [ ] Images of text patterns (Instagram)


## License

[MIT](https://choosealicense.com/licenses/mit/)