## CHECK 1 ##

def check_suspect_mappings_old(row, mdrefvaldict):
    temp_subjectcode = row['SubjectCode']
    temp_tfield = row['tField']
    temp_sValue = row['sValue']
    temp_tValue = row['tValue']

    if temp_subjectcode in mdrefvaldict:
        if temp_tfield in mdrefvaldict[temp_subjectcode]:
            #provjeri svalue
            if temp_sValue in mdrefvaldict[temp_subjectcode][temp_tfield]:
                if temp_sValue!=temp_tValue:
                    if temp_tValue in mdrefvaldict[temp_subjectcode][temp_tfield]:
                        status='also in EPI REF'
                    else:
                        status = 'not in EPI REF'
                    msg=f"WEIRD MAPPING: '{temp_sValue} (in EPI REF) > {temp_tValue} {status}. Why replace a valid value that exists in the REF table?"
                    print(msg)
                    return(msg)
        elif temp_tfield.replace('Code','') in mdrefvaldict[temp_subjectcode]:       
        #elif temp_tfield.replace('Code','')in temp_tfield:
            #provjeri
            temp_tfield = temp_tfield.replace('Code','')
            if temp_sValue in mdrefvaldict[temp_subjectcode][temp_tfield]:
                if temp_sValue!=temp_tValue:
                    if temp_tValue in mdrefvaldict[temp_subjectcode][temp_tfield]:
                        status='also in EPI REF'
                    else:
                        status = 'not in EPI REF'
                    msg=f"WEIRD MAPPING: '{temp_sValue} (in EPI REF) > {temp_tValue} {status}. Why replace a valid value that exists in the REF table?"
                    print(msg)
                    return(msg)
        else:
            msg=f"Couldn't find column {temp_tfield} for subject code {temp_subjectcode} in md.EPC_RefValue! Check is it a matter of variabledw vs variable code"
            print(msg)
            return None
    else:
        msg = f"Couldn't find subject code {temp_subjectcode} in md.EPC_RefValue!"
        print(msg)
        return None
    

def check_suspect_mappings(lookup_df, mdrefvaldict,epi_subject_ref_vardw_onlyref_dict):

    POOL_WARNINGS=[]
    POOL_OTHER=[]
    for x, row in lookup_df.iterrows():

        col_skip=['LocationCode','DataSourceCode', 'ReportingCountry','UploadRowNumber','UploadGuid','TimeCode','EpiValidationGuid']
        col_skip_dangerious = ['PathogenCode','HealthTopicCode']
        if (row['tField'] in (col_skip+col_skip_dangerious) or row['tField'].startswith('Place')  
            or row['tField'].startswith('Country') or row['tField'].startswith('Age') 
            or row['tField'].startswith('Date') or row['tField'].endswith('Id')
            or row['tField'].startswith('Valid')):
            continue

        temp_subjectcode = row['SubjectCode']
        temp_tfield = row['tField']
        temp_sValue = row['sValue']
        temp_tValue = row['tValue']

        

        if temp_subjectcode in mdrefvaldict:
            if temp_tfield in mdrefvaldict[temp_subjectcode]:
                #provjeri svalue
                if temp_sValue in mdrefvaldict[temp_subjectcode][temp_tfield]:
                    if temp_sValue!=temp_tValue:
                        if temp_tValue in mdrefvaldict[temp_subjectcode][temp_tfield]:
                            status='(also in EPI REF). Check the root cause for mapping a valid EPI REF value.'
                        else:
                            status = '(not in EPI REF). Check the root cause for mapping a valid EPI REF value.'
                        msg=f"{temp_subjectcode}: Value >{temp_sValue}< (in EPI REF) maps to >{temp_tValue}< {status}"
                        print(msg)
                        POOL_WARNINGS.append(msg)

            else:
                if temp_tfield in epi_subject_ref_vardw_onlyref_dict[temp_subjectcode]:
                    msg=f"{temp_subjectcode}: Couldn't find column {temp_tfield} in md.EPC_RefValue, although it exists as a REF in md.EPC_Variable!"
                    print(msg)
                    POOL_WARNINGS.append(msg)

                elif temp_tfield.endswith('Code'):
                    #temp_vartype = [temp_subjectcode]
                    msg=f"{temp_subjectcode}, VarDW: {temp_tfield} is not in md.EPC_RefValue and isn't of Vartype='REF' despite the '*Code' suffix."
                    POOL_OTHER.append(msg)
                else:
                    msg=f"{temp_subjectcode}: Couldn't find column {temp_tfield} in md.EPC_RefValue, but is not a REF in the md.EPC_Variable!"
                    print(msg)
        else:
            msg = f"Couldn't find subject code {temp_subjectcode} in md.EPC_RefValue!"
            print(msg)
            #RESULT_POOL.append(msg)

        #TODO - nesto iz REF a da ne postoji u ECP_REF

    return {'WARNINGS':sorted(POOL_WARNINGS),'OTHER':sorted(POOL_OTHER)}
    #df_lookup['REF_CHECK'] = df_lookup.apply(lambda row:check_suspect_mappings?old(row,refval), axis=1 )

    

    #####################################################

    ### CHECK 2 ##
    #TODO - uprizoriti u tablicu


#OVO JE SLICNO PRIJASNJEM CHECKU, PONAVLJAM SE!
def dlookup_missing_tfields(epi_subject_vardw_dict, dict_col_renaming):
    RESULT_POOL=[]
    def check(epi_subject_vardw_dict, dict_col_renaming,subjectcode):
        indlookup = set([x.lower() if x is not None else x for x in dict_col_renaming.keys()])
        #in_epi = set(dfepicurrent['VariableDW'].str.strip().str.lower().unique().tolist())
        in_epi = set([x.lower() if x is not None else x for x in epi_subject_vardw_dict[subjectcode]])
        #tbl=dfepicurrent['TableDW'].dropna().unique().tolist()[0]
        not_in_dlookup = in_epi-indlookup
        #BUG! bitno je znati za age code i agemonth! PREPOUSTIO SAM IH!
        #TODO - treba ih ocistiti a ne izbaciti!
        ####
        not_in_dlookup = not_in_dlookup - set(["cast(replace(agecode,'age','') as int)",'datasourcecode','agecode','nationalrecordid','timecode',None,"cast(replace(agemonthcode,'m','') as int)","agemonthcode","itemcode"])
        #### BUG linija povise - treba renameati a ne replaceati
        if len(not_in_dlookup)>0:
            res=f"{subjectcode}: SDW col {not_in_dlookup} missing in tField "
            print(res)
            print("------------------------ ")
            return res

    for subj in dict_col_renaming:
        res=check(epi_subject_vardw_dict, dict_col_renaming[subj],subj)
        if res is not None:
            RESULT_POOL.append(res)
    return RESULT_POOL

########################

### CHECK 3 ##########
def check_lookup_duplicates(df_lookup):
    CHECK_NAME = 'Duplicates'
    if df_lookup.duplicated().any():
        print("Dlookup has DUPLICATE rows")
        return "Dlookup has DUPLICATE rows"
    else:
        return "All good"