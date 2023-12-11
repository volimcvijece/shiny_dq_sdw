from pandas import merge, to_numeric

class GetResults():

    #def __init__(self,final_result_df):
    #    self.final_result_df=final_result_df

    def __init__(self,final_result_df):
        self.final_result_df=final_result_df

    def _check_fk_no_ref(self):
        
        #PK koji nisu oznaceni kao REF
        f1 = self.final_result_df['VariableType']!='REF'
        f2 = self.final_result_df['PK_Table'].notnull()
        #data_filtered = final_result_df[f1 & f2]

        #bitno! izbaciti LOCATION, DATASOURCE, DATE
        f3=~self.final_result_df['ColumnNames'].str.startswith('Date')#, 'ColumnNames'].unique()
        #f4=~final_result_df['ColumnNames'].str.startswith('CountryOf')#, 'ColumnNames'].unique()
        f4=~self.final_result_df['ColumnNames'].str.contains('Country')#, 'ColumnNames'].unique()
        #f5=~final_result_df['ColumnNames'].str.startswith('PlaceOf')#, 'ColumnNames'].unique()
        f5=~self.final_result_df['ColumnNames'].str.contains('Place')#, 'ColumnNames'].unique()
        f6=~self.final_result_df['ColumnNames'].isin(['ReportingCountry', 'DataSourceCode', 'TimeCode'])
        self.final_result_df['CHECK_FK_on_nonREF'] = f1 & f2 & f3 & f4 & f5 & f6

    def _check_ref_no_fk(self):
        #TEST 2 - naci REF koji nema FK!
        #ovdje ce svakako biti REPEATED VALUES (npr sa varchar(1000))
        #a neki stvarno FK nemaju tipa MENI ResultMLST
        f1 = self.final_result_df['VariableType']=='REF'
        f2 = self.final_result_df['PK_Table'].isnull()

        f3 = self.final_result_df['Repeatable']==False
        #data_filtered = final_result_df[f1 & f2]
        #DODATI JOS JEDAN KRITERIJ DA JE 'Repetable' 1????
        self.final_result_df['CHECK_REF_without_FK'] = f1 & f2 & f3

    #preskociti? trenutno u cleancolumns smo izbacili target max char
    def _check_consistency_maxlength_(self):
        #ako je text da li ima dokle moze varchar i poklapa li se

        f1=self.final_result_df['TextMaxLength']>self.final_result_df['TARGET_MAX_CHAR']
        f2=self.final_result_df['TextMaxLength'].notnull() & self.final_result_df['TARGET_MAX_CHAR'].isnull()
        #f3=final_result_df['TextMaxLength'].notnull()
        self.final_result_df['CHECK_small_db_text'] = f1|f2

    ##### !!!!!!!!!!!!!!! nedovrseno
    def _check_consistency_nullable(self):
        #required 1 znaci da ne smije biti nullable tj da je is nullable 0!!!! obrnuta logika! 
        # #OPREZ! - "IS NULLABLE" znaci da NIJE REQUIRED, tj IS NULLABLE TRUE mora biti REQUIRED False!   
        #final_result_df['TARGET_IS_NULLABLE'] = final_result_df['TARGET_IS_NULLABLE'].replace({1: True, 0: False,'1': True, '0': False,'Yes': True, 'No': False,0.0:False, 1.0: True, 'YES':True, 'NO':False})
        self.final_result_df['SDW_Required'] = self.final_result_df['TARGET_IS_NULLABLE'].replace({True:False, False:True,1: False, 0: True,'1': False, '0': True,'Yes': False, 'No': True,0.0:True, 1.0: False, 'YES':False, 'NO':True})    
        self.final_result_df['Required'] = self.final_result_df['Required'].replace({1: True, 0: False,'1': True, '0': False,'Yes': True, 'No': False,0.0:False, 1.0: True, 'YES':True, 'NO':False})

        #gledamo da nije isto jer smo promijenili IS_NULLABLE u REQUIRED
        f = self.final_result_df['Required'] != self.final_result_df['SDW_Required']
        self.final_result_df['CHECK_required_consistency'] = f

    def _check_missing(self):
        f = self.final_result_df['TABLE_NAME'].isnull()
        self.final_result_df['CHECK_missing_db_field'] = f
        #
        f = self.final_result_df['VariableCode'].isnull()
        self.final_result_df['CHECK_missing_meta_field'] = f


    def create_result_flags(self):
        self._check_missing()
        self._check_fk_no_ref()
        self._check_ref_no_fk()
        self._check_consistency_nullable()
        return self.final_result_df



class Prepare():
    #def __init__(self):
        #self.final_result_df=final_result_df



        #TODO - BITNO - these functions should accept 
        #draft excel files AND current DB META tables (same col names though)


    @staticmethod
    def enrich_info_schema(info_schema_whole_db_cleaned,meta_target_fk_all_tables):
        return merge(info_schema_whole_db_cleaned, meta_target_fk_all_tables,  how='left', left_on=['TABLE_NAME','COLUMN_NAME'], right_on = ['FK_Table','FK_Column'])



    @staticmethod
    def clean_epi_metadata(excel_meta_df):
        #TODO - health topic workaround, negdje ima negdje ne

        excel_meta_df['Required'] = excel_meta_df['Required'].replace({1: True, 0: False,'1': True, '0': False,'Yes': True, 'No': False,0.0:False, 1.0: True})

        excel_meta_df['TextMaxLength'] =to_numeric(excel_meta_df['TextMaxLength'], errors='coerce') #downcast='integer'
        #excel_meta_df['TARGET_MAX_CHAR'] =pd.to_numeric(excel_meta_df['TARGET_MAX_CHAR'],errors='coerce') #downcast='integer'

        ignorelist_metadata=['Status', 'Age', 'AgeMonth','AgeGroup', 'AgeCode']
        ignorelist_workaround = [] #'HealthTopicCode'
        return excel_meta_df[~excel_meta_df['VariableCode'].isin(ignorelist_metadata+ignorelist_workaround)]



    ##################


    @staticmethod
    def column_cleaning(test_result_epi_vs_db):
        cols_to_del = ['VariableDescription','UIPage', 'UIPosition','TextRegex', 'TextMaxLength', 'NumMinValue', 'NumMaxValue',
        'NumMinDecimals', 'NumMaxDecimals', 'DateAllowFutureValues',
        'DateMinValue', 'DateMaxValue', 'DateAllowYear', 'DateAllowQuarter',
        'DateAllowMonth', 'DateAllowWeek', 'DateAllowDay',
        'LocationIsUserCountry', 'LocationAllowSpecial', 'LocationAllowCountry',
        'LocationAllowNUTS1', 'LocationAllowNUTS2', 'LocationAllowNUTS3','TARGET_MAX_CHAR',
        'LocationAllowGAUL1', 'LocationAllowGAUL2', 'TABLE_CATALOG','ValidFrom','ValidTo',
        'TABLE_SCHEMA', 'TABLE_NAME', 'TARGET_DATA_TYPE','TARGET_DECIMAL_SCALE','Constraint_Name',
        'FK_Table', 'FK_Column','TableDW','VariableName', 'VariableTypeId','SubjectGuid',
    'VariableGuid','DateAllowedFormat','LocationAllowedType','UILabel','UIDescription','UIOverview','SchemaDW']
        test_result_epi_vs_db = test_result_epi_vs_db.drop(columns=cols_to_del,errors='ignore')

        #reorder columns
        maincols = ['TableNames','ColumnNames','SubjectCode','DiseaseCode','HealthTopicCode']
        checkscols = [i for i in test_result_epi_vs_db.columns if i.startswith('CHECK')]

        other_cols = [col for col in test_result_epi_vs_db.columns if col not in maincols+checkscols]

        #cols = list(test_result_epi_vs_db.columns)
        #cols = [cols[1]]+cols[-6:]+cols[2:-6]
        test_result_epi_vs_db = test_result_epi_vs_db[maincols+other_cols+checkscols]
        return test_result_epi_vs_db.sort_values(by=['SubjectCode', 'DiseaseCode'])



def get_test_result_epi_vs_db(epi_meta,db_meta_full):

    #epi_meta = EPIMDvsSDW.Prepare.clean_epi_metadata(epi_meta)
    epi_meta = Prepare.clean_epi_metadata(epi_meta)

    #ovo ne bi smjelo zvati pandas nepotrebno
    test_result_epi_vs_db = merge(epi_meta, db_meta_full,  how='outer', left_on=['TableDW','VariableDW'], right_on = ['TABLE_NAME','COLUMN_NAME'])


    test_result_epi_vs_db['TableNames'] = test_result_epi_vs_db['TableDW'].fillna(test_result_epi_vs_db['TABLE_NAME'])
    test_result_epi_vs_db['ColumnNames'] = test_result_epi_vs_db['VariableDW'].fillna(test_result_epi_vs_db['COLUMN_NAME'])

    #test_result_epi_vs_db = EPIMDvsSDW.GetResults.create_result_flags(test_result_epi_vs_db)
    #BUG #TODO - net performant! make it static!
    generate = GetResults(test_result_epi_vs_db)
    test_result_epi_vs_db = generate.create_result_flags()

    #ccheck._check_missing(test_result_epi_vs_db)
    #ccheck._check_fk_no_ref(test_result_epi_vs_db)
    #ccheck._check_ref_no_fk(test_result_epi_vs_db)
    #ccheck._check_consistency_nullable(test_result_epi_vs_db)


    #test_result_epi_vs_db = emv.result_create_flags(test_result_epi_vs_db)
    return Prepare.column_cleaning(test_result_epi_vs_db)
