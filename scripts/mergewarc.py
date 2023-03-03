from pathlib import Path
from crawchet.process import archive

if __name__ == '__main__':
    arcman = archive.ArchiveManager()

    arcdir = Path('../data/raw/pages')

    # TODO: script to construct all of these archives
    arcfiles = ['nearest_archives','patched_archives','only_wayback_replacements','all_wayback_urls']

    arcpaths = [arcdir.joinpath(arc).with_ext('.warc.gz') for arc in arcfiles]

    arcman.merge_archives(arcpaths, arcdir/'merged_archives.warc.gz')