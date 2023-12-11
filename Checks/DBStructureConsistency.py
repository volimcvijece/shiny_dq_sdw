#izvuci i HELPER_CHECK_TABLE_CONSISNT

def extract_columns_meta_whole_db():
    #iz helper check table consistency.py
    pass


def get_result(db_meta_columninfo):
    resultdf = db_meta_columninfo.pivot(index='COLUMN_NAME', columns='TABLENAMEFULL', values=['TARGET_DATA_TYPE_FULL'])
    resultdf['IS_CONSISTENT_FLAG']=resultdf.nunique(axis=1).eq(1)
    cols = list(resultdf.columns) #Shift last column to first
    cols = [cols[-1]] + cols[:-1]
    resultdf = resultdf[cols]
    resultdf.columns = [f'{j}' if j != '' else f'{i}' for i,j in resultdf.columns]
    return resultdf.reset_index()
