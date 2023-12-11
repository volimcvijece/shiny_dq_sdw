from utilsdb import DBConnection as db
from utilssdw import dlookuputils

from utilsdb import QueriesGeneral as g


#TODO failsafe ako se ne uspije querijati
#primjer zajeba - 
#Database 'EpiPulseCasesMetadata_workTC_20231106' cannot be opened because it is offline !!!!

#TODO - OVO LOADATI SAMO JEDNOM! i onda LOC-ati!
class MainTables():

    def __init__(self, server_env):
        self.df_dict={}
        self.server_env = server_env
        #self._load_subjectsref()

    def get_loadedtables_list(self):
        return self.df_dict.keys()
    
    def get_table_full(self, name):
        if name in self.df_dict:
            return self.df_dict[name]
        else:
            return [] 
            #raise error ne postoji tablica

    def get_table_by_dbname(self,name):
        if name in self.df_dict:
            if 'SubjectCode' in self.df_dict[name]:
                return self.df_dict[name].loc[self.df_dict[name]['SubjectCode'].isin(self.subjectlist),:]
            else:
                return self.df_dict[name]
        else:
            pass 
            #raise error ne postoji tablica
    

    # preduvjet, ne loada se zasebno
    def _load_subjectsref(self):
        name = 'subjectsref'
        connection = db.create_conn(self.server_env,'REF') #beggining or end?
        cursor = connection.cursor() 

        query_get_main_ref_tbl= f"""SELECT s1.SubjectCode, s1.DiseaseCode, s1.HealthTopicCode, s1.DiseaseProgrammeCode, s2.SubjectName, s2.DbDW, s2.SchemaDW, s2.TableDW FROM REF.ref.dSubjectTodDiseaseTodHealthTopic s1
        LEFT JOIN REF.ref.dSubject s2 ON s1.SubjectCode = s2.SubjectCode"""
        self.df_dict[name] = db.run_query_to_df(cursor,query_get_main_ref_tbl)
        db.close_con(connection)
        #glpo, potencijalni BUG, trebali bi subject popis uzet iz drugdje, previse ovisnosti
        #self.subjectlist = self.df_dict[name].loc[self.df_dict[name]['DiseaseProgrammeCode']==f'{self.db_name}','SubjectCode'].unique().tolist()
        #print("Subject list: ", self.subjectlist)

    #BUG u nastajanju - trebat ce otvoriti dvije konekcije
    def load_lookup_all(self):
        name = 'dlookup_all'
        connection = db.create_conn(self.server_env,'REF') #beggining or end?
        cursor = connection.cursor() 
        query_lookupmain = f"""
        SELECT * FROM REF.common.dLookup
        """
        df_lookup_main = db.run_query_to_df(cursor,query_lookupmain)
        db.close_con(connection)


        connection = db.create_conn('NVSQL3T','REF') #beggining or end?
        cursor = connection.cursor() 
        query_lookupsdw = f"""
        SELECT * FROM NSQL3.DM_Ref.sdw.dLookupSDW
        """
        df_lookup_roy =  db.run_query_to_df(cursor,query_lookupsdw)
        db.close_con(connection)


        df_lookups = dlookuputils.create_dlookup_final(df_lookup_main,df_lookup_roy) #TODO: dodati u dlookuputils!
        df_lookups = dlookuputils.clean_lookup_generic(df_lookups)
        df_lookups = dlookuputils.clean_lookup_split_tablename(df_lookups)
        #BUG potencijalni, odmah scopeam - ili odmah uzeti sve pa s obzirom
        #na DB scopeati ovdje a ne opet sve loadati? hm....
        #TODO - dizajn promisliti, ili odmah svaki df ikad pa u memoriji scopeati ili svaki
        #put querijati!
        #TODO TEMP
        #self.df_dict[name] = df_lookups.loc[df_lookups['SubjectCode'].isin(self.subjectlist),:]
        self.df_dict[name] = df_lookups


    
    

    
class Dynamic():
    def __init__(self, server_env, db_name):
        self.df_dict={}
        self.server_env = server_env
        self.db_name = db_name #opcionalno, bolje kroz funkcije!
        self.subjectlist = []

    def get_loadedtables_list(self):
        return self.df_dict.keys()
    
    def get_table(self, name):
        if name in self.df_dict:
            return self.df_dict[name]
        else:
            pass 
            #raise error ne postoji tablica


    #####################################################   







    #ovaj je za DBStructureConsistency
    def load_columninfo_by_db(self):
        name='columninfo_by_db'
        def _clean_info_schema_whole_db_tables(info_schema_whole_db):
        ###Table enrichment (concat varchar with max number, concat decimals with scale number )
            info_schema_whole_db['TARGET_DATA_TYPE_FULL']=(info_schema_whole_db['TARGET_DATA_TYPE']+'('+info_schema_whole_db['TARGET_MAX_CHAR'].astype(str).combine_first(info_schema_whole_db['TARGET_DECIMAL_SCALE']).astype(str)+')').where(info_schema_whole_db['TARGET_DATA_TYPE'].isin(['varchar', 'decimal']),info_schema_whole_db['TARGET_DATA_TYPE'])
            #TABLE_NAME, COLUMN_NAME

            #get rid of marts? #TODO
            marts = info_schema_whole_db['TABLE_NAME'].str.startswith('m')
            fme = info_schema_whole_db['TABLE_NAME'].str.startswith('FME')
            ignorelist_marts = info_schema_whole_db.loc[marts,'TABLE_NAME'].unique().tolist()
            ignorelist_fme = info_schema_whole_db.loc[fme,'TABLE_NAME'].unique().tolist()
            print("ignore marts:", ignorelist_marts)
            info_schema_whole_db =info_schema_whole_db[~(info_schema_whole_db['TABLE_NAME'].isin(ignorelist_marts) | info_schema_whole_db['TABLE_NAME'].isin(ignorelist_fme))] #prije: ~(all_tables_subset['COLUMN_NAME'].str.contains("CaseId") |  all_tables['COLUMN_NAME'].isin(ignorelist_infoschema))

        
            #valjda je regex dobar#|_RC
            temp_tables = info_schema_whole_db.loc[info_schema_whole_db['TABLE_NAME'].str.contains('diff|test|bkup|backup|bkp|temp|_rc_|sys|_RC', na=False, regex=True, case=False),'TABLE_NAME'].unique().tolist()
            print("ignore tables:", temp_tables)

            info_schema_whole_db =info_schema_whole_db[~(info_schema_whole_db['TABLE_NAME'].isin(temp_tables))] #prije: ~(all_tables_subset['COLUMN_NAME'].str.contains("CaseId") |  all_tables['COLUMN_NAME'].isin(ignorelist_infoschema))

        
            return info_schema_whole_db

        def _clean_info_schema_whole_db_columns(info_schema_whole_db):
            #age i AgeMonthCode postoje u metadata ali je napisan kao kod pa ne mogu joinati
            ignorelist_technicalcolumns=['ValidFrom', 'ValidTo', 'ValidFrom_VirtualDS', 'ValidTo_VirtualDS', 'EpiValidationGuid', 'UploadGuid', 'UploadRowNumber', 'RecordId'] # 'LocationCode', 'AgeCode', 'AgeMonthCode','AgeClassificationCode',

            #TODO - ovaj "CaseId" je upitan, izbacio sam ga
            info_schema_whole_db =info_schema_whole_db[~(info_schema_whole_db['COLUMN_NAME'].isin(ignorelist_technicalcolumns))] #prije: ~(all_tables_subset['COLUMN_NAME'].str.contains("CaseId") |  all_tables['COLUMN_NAME'].isin(ignorelist_infoschema))

        #hardcodes, , 
            #TODO - temp health topic to delete
            #health topic negdje ima negdje nema
            hardcodelist_correct = ['AgeClassificationCode','ParentIsolateId'] #'HealthTopicCode'
            info_schema_whole_db =info_schema_whole_db[~(info_schema_whole_db['COLUMN_NAME'].isin(hardcodelist_correct))] #prije: ~(all_tables_subset['COLUMN_NAME'].str.contains("CaseId") |  all_tables['COLUMN_NAME'].isin(ignorelist_infoschema))

            #BUG: Opasno za PathogenCode
            hardcodelist_workaround = ['LocationCode', 'AgeCode', 'AgeMonthCode'] #'PathogenCode'?
            info_schema_whole_db =info_schema_whole_db[~(info_schema_whole_db['COLUMN_NAME'].isin(hardcodelist_workaround))] #prije: ~(all_tables_subset['COLUMN_NAME'].str.contains("CaseId") |  all_tables['COLUMN_NAME'].isin(ignorelist_infoschema))

        #get rid of id columns
            idcols = info_schema_whole_db['TABLE_NAME'].unique().tolist()
            idcols = [x[1:]+'Id' for x in idcols]
            info_schema_whole_db =info_schema_whole_db[~(info_schema_whole_db['COLUMN_NAME'].isin(idcols))] #prije: ~(all_tables_subset['COLUMN_NAME'].str.contains("CaseId") |  all_tables['COLUMN_NAME'].isin(ignorelist_infoschema))


        
            return info_schema_whole_db

        connection = db.create_conn(self.server_env,self.db_name )
        cursor = connection.cursor()
        db_meta_columninfo = db.run_query_to_df(cursor, g.get_column_info_whole_db(self.db_name))
        db_meta_columninfo = _clean_info_schema_whole_db_tables(db_meta_columninfo)
        db_meta_columninfo = _clean_info_schema_whole_db_columns(db_meta_columninfo)

        self.df_dict[name]=db_meta_columninfo


    #TODO - nije dovoljno samo ime db-a jer unutra imamo MD.EPC_Variable i DBO.mdVariable - PITATI KOJA JE RAZLIKA?
    #ovo je dinamicki jer ovisi o db_name!
    #za EPIMDvsSDW (preduvjet za join)
    def load_epi_variable_customdb(self, custom_epi_table):
        #dbo.mdVariable a ne md.EPC_Variable
        name = 'custom_mdvariable'
        connection = db.create_conn(self.server_env,f'{self.db_name}') #beggining or end?
        cursor = connection.cursor() 
        epi_meta_query = f"""
            SELECT v.*, vt.VariableType, s.SubjectCode, s2.DiseaseCode, s2.HealthTopicCode 
            FROM {custom_epi_table}.dbo.mdVariable v
            --FROM [EpiPulseCasesDM].[md].[EPC_Variable] v
            LEFT JOIN {custom_epi_table}.dbo.mdSubject s ON s.SubjectGuid = v.SubjectGuid
            LEFT JOIN {custom_epi_table}.dbo.mdSubjectToDiseaseHealthTopic s2 ON s.SubjectGuid = s2.SubjectGuid
            LEFT JOIN {custom_epi_table}.dbo.mdVariableType vt ON vt.VariableTypeId = v.VariableTypeId
            WHERE v.DbDW = '{self.db_name}' --upitno, onda je ovo dinamicki?
            """
        #print("CUSTOM EPI QUERY: \n ", epi_meta_query)
        self.df_dict[name] = db.run_query_to_df(cursor, epi_meta_query)
        db.close_con(connection)


    #TODO - popraviti nazivlja
    #za EPIMDvsSDW (preduvjet za join)
    def load_meta_fkinfo(self):
        name='meta_fkinfo'
        connection = db.create_conn(self.server_env,self.db_name )
        cursor = connection.cursor()
        db_meta_fkinfo = db.run_query_to_df(cursor, g.get_column_info_fk())

        self.df_dict[name]=db_meta_fkinfo


    #za EPIMDvsSDW (final, join od gornja 2)
    def load_meta_full_enriched(self):
        from pandas import merge
        #TODO - realno uz ovo mogu izbrisati prvi i drugi, tj ne trebaju uopce!
        #samo trose memoriju!
        #TODO - bolja nazivlja!

        name='db_meta_full'

        if 'columninfo_by_db' in self.df_dict and 'meta_fkinfo' in self.df_dict:
            pass 
        else:
            #raiseerror
            print("PREREQUISITE TABLES ARE NOT LOADED!")
        self.df_dict[name]= merge(self.df_dict['columninfo_by_db'], self.df_dict['meta_fkinfo'],  how='left', left_on=['TABLE_NAME','COLUMN_NAME'], right_on = ['FK_Table','FK_Column'])




    #prije u staticki, potreban za dlookup (jer usporedujemo protiv EpipulseCasesDM.md.EPC_Variable a ne EpiPulseCasesMetadata.dbo.EPC_Variable)
    def load_epidm_epcvariable(self):
        name = 'epidm_epc_variable'
        connection = db.create_conn(self.server_env,'EpiPulseCasesDM') #beggining or end?
        cursor = connection.cursor() 
        query_epivars = f"""
        SELECT vt.VariableType, v.* FROM [EpiPulseCasesDM].[md].[EPC_Variable] v
        LEFT JOIN EpiPulseCasesMetadata.dbo.mdVariableType vt ON v.VariableTypeId = vt.VariableTypeId 
        WHERE v.ValidTo IS NULL
        AND v.DbDW = '{self.db_name}'
        --AND v.SubjectCode IN ({','.join([f"'{x}'" for x in self.subjectlist])})
        """
        self.df_dict[name] = db.run_query_to_df(cursor,query_epivars)
        db.close_con(connection)


        #epi_subjects_supported_list = df_epi_variables['SubjectCode'].unique().tolist()
        #epi_subject_ref_varcode_dict = df_epi_variables.loc[df_epi_variables['VariableTypeId']==2,:].groupby('SubjectCode')['VariableCode'].apply(list).to_dict()
        #epi_subject_ref_vardw_dict = df_epi_variables.groupby('SubjectCode')['VariableDW'].apply(list).to_dict() #df_epi_variables.loc[df_epi_variables['VariableTypeId']==2,:]
        #epi_subject_ref_vardw_onlyref_dict = df_epi_variables.loc[df_epi_variables['VariableTypeId']==2,:].groupby('SubjectCode')['VariableDW'].apply(list).to_dict() #


    def load_epidm_refval(self):

        name = 'epidm_refval'
        connection = db.create_conn(self.server_env,'EpiPulseCasesDM') #beggining or end?
        cursor = connection.cursor() 
        query_epiref = f"""
        SELECT r.*, v.VariableDW FROM [EpiPulseCasesDM].md.EPC_RefValue r
        LEFT JOIN [EpiPulseCasesDM].md.EPC_Variable v ON r.VariableGuid = v.VariableGuid
        WHERE r.ValidTo IS NULL
        AND v.DbDW = '{self.db_name}'
        --AND r.SubjectCode IN ({','.join([f"'{x}'" for x in self.subjectlist])})
        """
        self.df_dict[name] = db.run_query_to_df(cursor,query_epiref)
        db.close_con(connection)

        #3 kolone u nested dict
        #refval = df_epi_referencevalues.groupby('SubjectCode').apply(lambda x: pd.DataFrame(zip(x['VariableCode'], x['RefValueCode'])).groupby(0)[1].apply(list).to_dict()).to_dict()
