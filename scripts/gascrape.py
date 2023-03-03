import json
from pathlib import Path

from crawchet.process import greatami
from crawchet.collect import scrape
from crawchet.utils.io import resolve_path

if __name__ == '__main__':
    warc_out = resolve_path('../data/raw/pages/greatamigurumi.warc.gz')
    json_out = resolve_path('../data/raw/urls/greatamigurumi.json')
    #flat_out = Path('../data/raw/urls/xgreatamigurumi_flat.json').resolve().as_posix()
    img_dir =  resolve_path('../data/raw/images/greatamigurumi/')
    
    ga_scraper = scrape.GreatAmigurumiScraper()
    gablog_data = ga_scraper.scrape(warc_out, timeout=1)
    
    with open(json_out,'w',encoding='UTF-8') as f:
        json.dump(gablog_data, f)
    

    df_gaf = greatami.process_gafile(ga_file=json_out)
    raw_images,bodyex_images,bodyex_ilinks = greatami.extract_links(df_gaf)

    gaimg_path = Path(img_dir)
    scrape.dl_greatami_imgs(raw_images, df_gaf['dirslug'], gaimg_path, overwrite=False)
    scrape.dl_greatami_imgs(bodyex_images, df_gaf['dirslug'], gaimg_path, overwrite=False)
    scrape.dl_greatami_imgs(bodyex_ilinks, df_gaf['dirslug'], gaimg_path, overwrite=False)