from shiny import App, render, ui,reactive,render
from pandas import merge, DataFrame
import shinyswatch

#TODO - kako se shiny online odnosi sa pozivanjem eksternih librarya sa gita? (utilsdb) - pogledati https://shinylive.io/py/examples/#extra-packages
from utilsdb import DBConnection as db
from utilsdb import QueriesGeneral as g

from CORE import Loader

from Checks import EPIMDvsSDW
from Checks import DBStructureConsistency
from Checks import DLookup


# DECISION - seperate shiny per ENVIRONMENT. That is, environment should be hardcoded!
CONN_ENV = 'NVSQL3T'

#disease_db_name = input.disease_db_name()
#epimeta_db_name = input.epimeta_db_name()
#epimeta_db_name = input.epimeta_db_name()


## STATIC TABLES (for the whole session) ##
tables_loader_static = Loader.MainTables(CONN_ENV)


#epimd_subjects_supported_list = tables_loader_static.get_table('epidm_epc_variable')['SubjectCode'].unique().tolist()
#epimd_subject_ref_vardw_onlyref_dict = tables_loader_static.get_table('epidm_epc_variable').loc[tables_loader_static.get_table('epidm_epc_variable')['VariableTypeId']==2,:].groupby('SubjectCode')['VariableDW'].apply(list).to_dict() #
#epimd_subject_ref_vardw_dict = tables_loader_static.get_table('epidm_epc_variable').groupby('SubjectCode')['VariableDW'].apply(list).to_dict() #df_epi_variables.loc[df_epi_variables['VariableTypeId']==2,:]




###############################################################################################



#app_ui = ui.page_fluid(
app_ui = ui.page_fluid(
        shinyswatch.theme.journal(), #FORA: united, minty, superhero, journal
        ui.panel_title("SDW Utilities - NVSQL3T", "SDW Utilities"),
        ui.layout_sidebar(
            ui.panel_sidebar(
                ui.input_select("disease_db_name", "Database name", choices=['AMR', 'VPD', 'FWD', 'EVD','COVID19']), #TODO - value dinamican drop down
                #ui.input_text("epimeta_db_name", "EPI db name", value=""), #TODO - value dinamican drop down
                ui.input_select("epimeta_db_name", "EPI db name", choices=['EpiPulseCasesMetadata', 'EpiPulseCasesMetadata_workCA','EpiPulseCasesMetadata_workTC_20231106']), #TODO - value dinamican drop down
                ui.input_action_button("run", "Run"),
                ui.output_text_verbatim("get_isdataloaded_outputtext"),
                width = 2,
            ),
            ui.panel_main(
                ui.page_navbar(
                    ui.nav(" SDW Consistency ",
                        ui.page_fluid(
                            ui.h3("Field consistency accross all tables in a db"),
                                ui.output_text_verbatim("desc1"),
                                ui.row(
                                    ui.column(4, ui.input_switch("filter_errors_only", "Display only flagged columns"))      
                                ),ui.br(),
                                ui.output_data_frame("output_table_consist"), #"grid"
                                ui.panel_fixed(
                                    ui.output_text_verbatim("detail"),
                                    right="10px",
                                    bottom="10px",
                                    ),
                                ),
                            ),
            
                    ui.nav(
                        " EPI vs SDW Cross check ", #navbar 2
                        ui.page_fluid(
                            ui.h3("EPI Metadata vs. SDW tables cross checking"),
                            ui.navset_tab(
                                ui.nav(
                                    "Details",
                                    ui.h3("Table output (details)"),
                                    ui.output_text_verbatim("desc2"),
                                    ui.output_data_frame("output_table_crosscheck"), #"grid"
                                    ),
                                ui.nav(
                                    "Summary",
                                    ui.h2("Result summary"),ui.br(),ui.br(),ui.br(),
                                    ui.h4("Missing variables in EPI metadata"),
                                    ui.output_text_verbatim("res_missingmeta_txt"),
                                    ui.output_table("res_missingmeta_df"),
                                    ui.br(),ui.br(),
                                    ui.h4("Missing variables in SDW tables"),
                                    ui.output_text_verbatim("res_missingdb_txt"),
                                    ui.output_table("res_missingdb_df"),
                                    ui.br(),ui.br(),
                                    ui.h4("SDW columns with FK that are not REF type in the EPI MD"),
                                    ui.output_text_verbatim("res_fknoref_txt"),
                                    ui.output_table("res_fknoref_df"),
                                    ui.br(),ui.br(),
                                    ui.h4("EPI MD non repeatable REF variables without FK"),
                                    ui.output_text_verbatim("res_refnofk_txt"),
                                    ui.output_table("res_refnofk_df"),
                                    ui.br(),
                                    ui.h4("Consistency in REQUIRED values"),
                                    ui.output_text_verbatim("res_requiredcon_txt"),
                                    ui.output_table("res_requiredcon_df"),
                                    ui.br(),

                                )
                            ),
                        ),
                     ),
                ui.nav(" DLOOKUP ",
                    ui.page_fluid(    
                        ui.h2("Various Dlookup discrepancies"),
                        ui.row(
                            ui.column(3,ui.input_action_button("get_dlookup_data_btn", "Load Dlookup data")),
                            ui.column(3,ui.input_action_button("get_result_dlookup_btn", "Get result")),
                            ),ui.br(), 
                        ui.h4("Dlookup warnings"),
                        ui.output_text_verbatim("get_dlookup_ref_warnings"),ui.br(),
                        ui.h4("Missing tFields from the dlookup AGAINST MD! TODO - against SDW"),                   
                        ui.output_text_verbatim("get_dlookup_ref_missingtfields"),ui.br(),
                       ) ,




                #title="Test",
                ),
            ),
        ),
    ),
)


           



def server(input, output, session):
    result_dbconsticency = reactive.Value()
    result_epi_vs_sdw = reactive.Value()

    result_dlookupref_warnings = reactive.Value()
    result_dlookupref_other = reactive.Value()
    results_dlookup_missingtfields = reactive.Value()

    epimd_subject_colmap_refonly = reactive.Value() 
    epimd_subject_colmap_allvar = reactive.Value() 
    epimd_refvalues = reactive.Value()
    #
    info_window = reactive.Value()

    ##dakle, i botun (press) i mjesto za pisanje (vrijednost iz njega) se dobijaju kroz "input" 

    ##ova funkcija se nigdje ne poziva vec je REACTIVE na press od botuna "run", 
    #tj ui.input_action_button("run", "Run")
    @reactive.Effect
    @reactive.event(input.run)
    def get_sql_data():
        #mozemo unutar funkcije dobiti podatke iz text boxa > ui.input_text("db_name", "Database name", value="")

        #TODO - razdvojiti main data tako da ga mogu shareati i ostali checkovi
        #a za stalno u reactive.Value() staviti samo one koji se prikazuju na dahsboardima

        disease_db_name = input.disease_db_name()
        epimeta_db_name = input.epimeta_db_name()


        #tables_loader_dynamic = Loader.Dynamic(CONN_ENV, DISEASE_PROGRAMME_DB)
        tables_loader_dynamic = Loader.Dynamic(CONN_ENV, disease_db_name)


        tables_loader_dynamic.load_columninfo_by_db()

        ######### GET DATAFRAMES FOR EACH TAB 
        #### 1- DB structure
        resultdf =  DBStructureConsistency.get_result(tables_loader_dynamic.get_table('columninfo_by_db'))
        result_dbconsticency.set(resultdf)


        #### 2- Epi vs SDW data
        tables_loader_dynamic.load_meta_fkinfo()
        tables_loader_dynamic.load_meta_full_enriched()

        tables_loader_dynamic.load_epi_variable_customdb(epimeta_db_name)

        test_result_epi_vs_db = EPIMDvsSDW.get_test_result_epi_vs_db(tables_loader_dynamic.get_table('custom_mdvariable')
                                                             ,tables_loader_dynamic.get_table('db_meta_full'))

        result_epi_vs_sdw.set(test_result_epi_vs_db)


        #tables_loader_dynamic.load_epidm_refval()

        info_window.set(f"{disease_db_name} data is loaded!")

        tables_loader_dynamic.load_epidm_epcvariable()

        epimd_subject_colmap_refonly.set(tables_loader_dynamic.get_table('epidm_epc_variable').loc[tables_loader_dynamic.get_table('epidm_epc_variable')['VariableTypeId']==2,:].groupby('SubjectCode')['VariableDW'].apply(list).to_dict()) #
        epimd_subject_colmap_allvar.set(tables_loader_dynamic.get_table('epidm_epc_variable').groupby('SubjectCode')['VariableDW'].apply(list).to_dict()) #df_epi_variables.loc[df_epi_variables['VariableTypeId']==2,:]

        tables_loader_dynamic.load_epidm_refval()

        epimd_refvalues.set(tables_loader_dynamic.get_table('epidm_refval').groupby('SubjectCode').apply(lambda x: DataFrame(zip(x['VariableDW'], x['RefValueCode'])).groupby(0)[1].apply(list).to_dict()).to_dict())

    @reactive.Effect
    @reactive.event(input.get_dlookup_data_btn)
    def get_lookup_data():

        #NE ovdje, opet se ponavlja, treba gore
        tables_loader_static.load_lookup_all()
        info_window.set(f"Dlookup table is loaded")

    @reactive.Effect
    @reactive.event(input.get_result_dlookup_btn)
    def get_results_dlookup():
        from utilssdw import dlookuputils

        if len(tables_loader_static.get_table_full('dlookup_all'))>1:
        #df_lookup_scoped = (tables_loader_static.get_table_full('dlookup_all').loc[tables_loader_static.get_table_full('dlookup_all')['SubjectCode'].isin(self.subjectlist),:])
            df_lookup_scoped = tables_loader_static.get_table_full('dlookup_all').loc[tables_loader_static.get_table_full('dlookup_all')['tDatabase']==input.disease_db_name(),:]
        ### sve u istu ili svatko sa svojim outputom? 

            ### RES 1
            results_weirdmapping = DLookup.check_suspect_mappings(df_lookup_scoped
                                                                ,epimd_refvalues.get(),epimd_subject_colmap_refonly.get())
            result_dlookupref_warnings.set(results_weirdmapping['WARNINGS'])
            result_dlookupref_other.set(results_weirdmapping['OTHER'])

            ### RES 2 
            dict_col_renaming={}
            for subj in epimd_subject_colmap_allvar.get().keys():
                temp_subset = tables_loader_static.get_table_full('dlookup_all').loc[tables_loader_static.get_table_full('dlookup_all')['SubjectCode']==subj]
                #dict_col_renaming[subj] = dlookuputils.get_column_mapping_dict(df_lookup)
                dict_col_renaming[subj] = dlookuputils.get_column_mapping_dict(temp_subset)

            results_dlookup_missingtfields.set(DLookup.dlookup_missing_tfields(epimd_subject_colmap_allvar.get(), dict_col_renaming))
        else:
            info_window.set(f"Dlookup table isn't loaded yet!")


    @reactive.Calc
    def filter_result_errors_only():
        resultdf = result_dbconsticency.get()
        subset_errors_only=input.filter_errors_only()


        if subset_errors_only:
            resultdf=resultdf.loc[resultdf['IS_CONSISTENT_FLAG'].isin([False, 'FALSE', 'false', 'False']),:]
        

        return resultdf




    @output
    @render.data_frame
    def output_table_consist():
        #resultdf = result_dbconsticency.get()
        resultdf = filter_result_errors_only()
        #height = 350 if input.fixedheight() else None
        #width = "100%" if input.fullwidth() else "fit-content"
        height = 450
        width = "100%"
        return render.DataGrid(
            resultdf,
            row_selection_mode="single",#input.selection_mode(),
            height=height,
            width=width,
            filters=True,#input.filters(),
        )
    
    @output
    @render.data_frame
    def output_table_crosscheck():
        test_result_epi_vs_db = result_epi_vs_sdw.get()
        #height = 350 if input.fixedheight() else None
        #width = "100%" if input.fullwidth() else "fit-content"
        height = 450
        width = "100%"
        return render.DataGrid(
            test_result_epi_vs_db,
            row_selection_mode="single",#input.selection_mode(),
            height=height,
            width=width,
            filters=True,#input.filters(),
        )
    
    ###probati neki od ovih hardcoded texta staviti odmah u UI jer ovo samo krci
    @output
    @render.text
    def desc1():
        return "Eyeballing for data type consistency for the same variables accross different SDW tables."
    
    @output
    @render.text
    def desc2():
        return "Eyeballing for consistency between EPI metadata and SDW physical tables \n (do we have some columns ommited or extra, are they correctly flagged as REF data or as required columns)"

    @output
    @render.text
    def res_missingmeta_txt():
        return f"SDW table columns not found in the EPI metadata"


    @output
    @render.text
    def res_missingdb_txt():
        return f"EPI metadata variables not found in the SDW tables"

    @output
    @render.text
    def res_fknoref_txt():
        return f"SDW table columns with foreign keys that are not REF type in the EPI md"



    @output
    @render.text
    def res_refnofk_txt():
        return f"EPI Metadata variables that are of non repeatable REF type that do not have foreign keys in SDW tables"
    

    @output
    @render.text
    def res_requiredcon_txt():
        return f"EPI Metadata variables with REQUIRED flag incompatible to NULL constraint in SDW"

    


    @output
    @render.table
    def res_missingmeta_df():
        test_result_epi_vs_db = result_epi_vs_sdw.get()


        return test_result_epi_vs_db.loc[(test_result_epi_vs_db['CHECK_missing_meta_field']==True) & (test_result_epi_vs_db['ColumnNames']!='HealthTopicCode'),['TableNames','ColumnNames']].drop_duplicates().groupby('TableNames').ColumnNames.apply(list).reset_index()


    @output
    @render.table
    def res_missingdb_df():
        test_result_epi_vs_db = result_epi_vs_sdw.get()
        return test_result_epi_vs_db.loc[test_result_epi_vs_db['CHECK_missing_db_field']==True,['TableNames','ColumnNames']].drop_duplicates().groupby('TableNames').ColumnNames.apply(list).reset_index()


    @output
    @render.table
    def res_fknoref_df():
        #logika - HealthTopicCode ne postoji u EPI jer se ne izvjestava?
        test_result_epi_vs_db = result_epi_vs_sdw.get()
        return test_result_epi_vs_db.loc[(test_result_epi_vs_db['CHECK_FK_on_nonREF']==True)& (test_result_epi_vs_db['ColumnNames']!='HealthTopicCode'),['TableNames','ColumnNames']].drop_duplicates().groupby('TableNames').ColumnNames.apply(list).reset_index()


    @output
    @render.table
    def res_refnofk_df():
        test_result_epi_vs_db = result_epi_vs_sdw.get()
        return test_result_epi_vs_db.loc[test_result_epi_vs_db['CHECK_REF_without_FK']==True,['TableNames','ColumnNames']].drop_duplicates().groupby('TableNames').ColumnNames.apply(list).reset_index()
        
        #test_result_epi_vs_db = result_epi_vs_sdw.get().loc[result_epi_vs_sdw.get()['CHECK_REF_without_FK']==True,:]
        #if len(test_result_epi_vs_db) >0:
        #    return test_result_epi_vs_db.loc[test_result_epi_vs_db['CHECK_REF_without_FK']==True,['TableNames','ColumnNames']].drop_duplicates().groupby('TableNames').ColumnNames.apply(list).reset_index()
        #else:
        #    return "All good!"
        


    @output
    @render.table
    def res_requiredcon_df():
        test_result_epi_vs_db = result_epi_vs_sdw.get()
        return test_result_epi_vs_db.loc[test_result_epi_vs_db['CHECK_required_consistency']==True,['TableNames','ColumnNames']].drop_duplicates().groupby('TableNames').ColumnNames.apply(list).reset_index()



    @output
    @render.text
    def get_isdataloaded_outputtext():
        return info_window.get()
    
    
    @output
    @render.text
    def get_dlookup_ref_warnings():
        if len(result_dlookupref_warnings.get())>0:
            return ('\n'.join(str(v) for v in result_dlookupref_warnings.get()))
        else:
            return "All good!"

    @output
    @render.text
    def get_dlookup_ref_other():
        return result_dlookupref_other.get()
    
    @output
    @render.text
    def get_dlookup_ref_missingtfields():
        if len(results_dlookup_missingtfields.get()) >0:
            return ('\n'.join(str(v) for v in results_dlookup_missingtfields.get()))
        else:
            return "All good!"
         
    


app = App(app_ui, server)
