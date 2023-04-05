import argparse

from crawchet.process import transform

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--gafile', type=str, default='../data/interim/greatamigurumi.json')
    parser.add_argument('--warc', type=str, default='../data/interim/merged.warc.gz')
    parser.add_argument('--df_out', type=str, default='../data/interim/df_master.pkl')
    parser.add_argument('--simphtml_out', type=str, default='../data/staged/simphtml.json')
    parser.add_argument('--tag_transform', type=str, default='fast')
    parser.add_argument('--text_only', type=bool, default=True)
    parser.add_argument('--max_status', type=int, default=399)
    parser.add_argument('--min_term_count', type=int, default=0)
    return parser

if __name__ == '__main__':
    args = get_parser().parse_args()
    #df = transform.warc_to_dftext(args.warc, args.tag_transform, args.text_only, args.max_status, args.min_term_count)
    
    df_master = transform.build_masterframe(args.gafile, args.warc, args.df_out, args.tag_transform)
    df_master = transform.parallel_simplify(args.df_out, args.simphtml_out, n_jobs=-5)
    
    #df.to_csv('../data/interim/df_textracts.csv', index=False)