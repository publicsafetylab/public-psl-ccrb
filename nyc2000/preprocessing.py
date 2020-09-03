# %%
import pandas as pd

# %%
dfs = []
for b in ['bk', 'bx', 'mn', 'qn', 'si']:
    dfs.append(pd.read_excel(
      f'./raw/sf1p10{b}.xls',
      header=5,
      dtype={'Borough': object, 'Census Tract': object, 'Census Block': object}))
df = pd.concat(dfs).dropna(how='all').rename(columns={
  'Unnamed: 3': 'Total Population',
  'Unnamed: 10': 'Two or More Races Nonhispanic',
  'Unnamed: 11': 'Hispanic Origin (of any race)',
  'Unnamed: 12': 'Total Housing Units'
  })

# %%
blocks = pd.read_csv(
  './raw/TAB2000_TAB2010_ST_36_v2.txt',
  dtype={
    'STATE_2000': object,
    'COUNTY_2000': object, 
    'TRACT_2000': object, 
    'BLK_2000': object,
    'STATE_2010': object,
    'COUNTY_2010': object, 
    'TRACT_2010': object, 
    'BLK_2010': object
    })

# %%
blocks['BLKID_2000'] = blocks[['TRACT_2000', 'BLK_2000']].apply(lambda x: ''.join(x), axis=1)
df['BLKID'] = df[['Census Tract', 'Census Block']].apply(lambda x: ''.join(x), axis=1)
nycblocks = df['BLKID'].unique()
blocks = blocks[blocks['BLKID_2000'].isin(nycblocks)]

# %%
blkprecinct = pd.read_csv(
  './precinct_block_key_2020.csv',
  dtype={'geoid10': object})
blocks['geoid10'] = blocks[['STATE_2010',	'COUNTY_2010',	'TRACT_2010',	'BLK_2010']].apply(lambda x: ''.join(x), axis=1)
blocks['geoid00'] = blocks[['STATE_2000',	'COUNTY_2000',	'TRACT_2000',	'BLK_2000']].apply(lambda x: ''.join(x), axis=1)
blocks = pd.merge(blocks, blkprecinct, how='left', on='geoid10')

# %%
blocks = pd.merge(blocks, df, how='left', left_on='BLKID_2000', right_on='BLKID')

# %%
blocks_precincts = blocks[['geoid10',
       'geoid00', 'precinct_2020',
       'Total Population', 'White', '   Black/ African American',
       '     American Indian and Alaska Native', '      Asian',
       'Native Hawaiian and Other Pacific Islander', '  Some Other Race',
       'Two or More Races Nonhispanic', 'Hispanic Origin (of any race)',
       'Total Housing Units']].dropna(subset=['precinct_2020'])

# %%

# %%
bp = blocks_precincts.drop_duplicates(subset=['geoid00', 'precinct_2020'])
uniq_prec = bp.groupby('geoid00').agg({'precinct_2020': 'nunique'}).rename(columns={'precinct_2020': 'uniq_prec'})
bp = pd.merge(bp, uniq_prec, how='left', on='geoid00')
bp.to_csv('blocks00_precincts20.csv', index=False)
# %%
bp = bp[bp['uniq_prec'] == 1].groupby('precinct_2020').sum().drop(columns=['uniq_prec'])
bp = bp.rename(columns=lambda x: x.strip())
bp = bp.rename(columns={
  'White': 'NH_White_00',
  'Black/ African American': 'Black_00',
  'Asian': 'NH_Asian_00',
  'Hispanic Origin (of any race)': 'Hispanics_00',
  'Total Population': 'Total_Population_00'
})
bp['Others_00'] = bp[['American Indian and Alaska Native', 'Native Hawaiian and Other Pacific Islander', 'Some Other Race']].sum(axis=1)
bp = bp[['Total_Population_00', 'Black_00', 'Hispanics_00', 'NH_Asian_00', 'NH_White_00', 'Others_00']]
for col in ['Black_00', 'Hispanics_00', 'NH_Asian_00', 'NH_White_00', 'Others_00']:
    bp[col+'_Percentage'] = bp[col] / bp['Total_Population_00']
bp.to_csv('precinct20_demos00.csv')
# %%
