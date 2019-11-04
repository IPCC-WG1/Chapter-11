import parse
import pandas as pd

def parse_ens(filelist):


    ens = filelist.df["ens"]

    if "f" in ens.iloc[0]:
        parser = parse.compile("r{r}i{i}p{p}f{f}")
    else:
        parser = parse.compile("r{r}i{i}p{p}")

    out = list()
    for i, one_ens in zip(ens.index, ens):
        parsed = parser.parse(one_ens)
        out.append(list(parsed.named.values()))

    keys = list(parsed.named.keys())

    df = pd.DataFrame(out, columns=keys)


    filelist.df = filelist.df.assign(**df)
    return filelist


def create_ensnumber(filelist, keys=['exp', 'table', 'varn', 'model']):


    df = filelist.df
    combined = filelist.combine_by_key(keys)

    df['ensnumber'] = -1

    for comb in combined.unique():

        sel = combined == comb
        numbers = list(range(sel.sum()))
        df.loc[sel, 'ensnumber'] = numbers

    filelist.df = df
    return filelist
