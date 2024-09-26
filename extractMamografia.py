import pandas as pd
import datetime
import duckdb
import platform
import cx_Oracle
from sqlalchemy import types, create_engine, text
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import os
import sys
from dotenv import load_dotenv
load_dotenv()
import logging
import warnings
warnings.filterwarnings('ignore')

"""
||#################################################################################
||Autor       : Daniel                                                           ||
||Empresa     : UNIMED                                                           ||
||Versao      : v1                                                               ||
||Data        : 2024-08-27                                                       ||
||Projeto     : Faz a extração dos dados e cria a tabela para visualização       ||
|| Tipo       : Evolutiva                     Evolutiva/Corretiva/Paleativa      ||
################################################################################### 
||***********************************    ****************************************||
||  ALTERACOES :                                                                 ||
||    DATA        VERSAO      RESPONSAVEL        TIPO     MOTIVO                 || 
||  ----------    ------     -----------------   ----     -----------------------||
||  dd/mm/yyyy     v2        RESPONSAVEL                                         ||
||*******************************************************************************||
"""



cx_Oracle.init_oracle_client(
    lib_dir=r"C://instantclient_19_6")


log_format = '%(asctime)s:%(levelname)s:%(filename)s:%(message)s:%(lineno)d'
logging.basicConfig(
    filename=r'D://projectPython//schedulersPython//Project_ProcessingMamografia014//log//processingLogs.log',
    encoding='utf-8',
    level=logging.INFO, 
    filemode='w',
    format=log_format)
logger = logging.getLogger('root')


def conectionDuckDB():
    """Conecta ao banco de dados DuckDB: **Cria o banco de dados se não existir"""
    return duckdb.connect(database='D://projectPython//schedulersPython//Project_ProcessingMamografia014//storageDB//tbProcessingMamo.db', read_only=False)

def conectDWUnimed():
    try:
        vServerName     = os.environ.get("SQLSERVER_HOST_DW")
        vDBName         = os.environ.get("SQLSERVER_DATABASE_DW")
        vUserName       = os.environ.get("SQLSERVER_USER_DW")
        vPassword       = os.environ.get("SQLSERVER_PASSWORD_DW")
        vPort           = '1433'
        
        from sqlalchemy import create_engine
        vUrl = 'mssql+pymssql://'+vUserName+':'+vPassword+'@'+vServerName+':'+vPort+'/'+vDBName
        engine = create_engine(vUrl)
        
        return engine
        
    except Exception as e:
        logging.error(f'{e} - ERROR CONEXAO DWUNIMED')
        print(f'{e} - ERROR CONEXAO SQLSERVER')  

def connectionMV():
    try:
        vServerName     = os.environ.get("ORACLE_MV_HOST")
        vDBName         = os.environ.get("ORACLE_MV_DATABASE")
        vUserName       = os.environ.get("ORACLE_MV_USER")
        vPassword       = os.environ.get("ORACLE_MV_PASSWORD")
        vPort           = '1521'
        
        from sqlalchemy import create_engine
        vURL = 'oracle+cx_oracle://'+vUserName+':'+vPassword+'@'+vServerName+'/?service_name='+vDBName
        engine = sqlalchemy.create_engine(vURL, arraysize=1000)
        
        return engine
    
    except Exception as e:
        logging.error(f'{e} - ERROR CONEXAO ORACLEMV')
        print(f'{e} - ERROR CONEXAO ORACLEMV')  

def connectionMVExec():
    try:
        vServerName     = os.environ.get("ORACLE_MV_HOST")
        vDBName         = os.environ.get("ORACLE_MV_DATABASE")
        vUserName       = os.environ.get("ORACLE_MV_USER")
        vPassword       = os.environ.get("ORACLE_MV_PASSWORD")
        vPort           = '1521'
        vSID            = 'SOULPRD'
        
        from sqlalchemy import create_engine
        dsn_tns = cx_Oracle.makedsn(vServerName, vPort, vSID)
        engine = create_engine(f'oracle+cx_oracle://{vUserName}:{vPassword}@{dsn_tns}')

        #vURL = 'oracle+cx_oracle://'+vUserName+':'+vPassword+'@'+vServerName+'/?service_name='+vDBName
        #engine = sqlalchemy.create_engine(vURL, arraysize=1000)
        
        return engine
    
    except Exception as e:
        logging.error(f'{e} - ERROR CONEXAO ORACLEMV')
        print(f'{e} - ERROR CONEXAO ORACLEMV')    

def conectarLog():
        vServerName     = os.environ.get("ORACLE_MV_HOST")
        vDBName         = os.environ.get("ORACLE_MV_DATABASE")
        vUserName       = os.environ.get("ORACLE_MV_USER")
        vPassword       = os.environ.get("ORACLE_MV_PASSWORD")
        vPort           = os.environ.get("ORACLE_MV_PORT")

        
        try:
            vUrl = vUserName+'/'+vPassword+'@'+vServerName+':'+vPort+'/'+vDBName
            con = cx_Oracle.connect(vUrl)
            
            return con
        except Exception as e:
            logging.error(f'{e} - ERROR CONEXAO ORACLE')

def geraLogProcess(vStart):
        connL = conectarLog()#conectarLog()#conectarLog()
        cursor = connL.cursor()

        # Definindo os parâmetros
        vProcesso = 'PythonProcess ProcessingMamografia_MV-Processo Diario'
        
        if vStart == 1:
            vStep = 1
        elif vStart == 2:
            vStep = 2
        else:
            vStep = 3

        query = (f"begin PRC_LOG_ETL_PENTAHO('{vProcesso}',{vStep}); end;")
        # Executando a procedure
        cursor.execute(query)
        # Commitando a transação
        connL.commit()
        # Fechando a conexão
        connL.close()                

def creatTablePopTmp(vDf,vComp):
    engine = None
    conn = None
    try:
        engine = connectionMV()
        if engine is None:
            raise Exception("Falha na conexão com o banco de dados")
        
        table_name = 'TMP_TB_POPULACAO_MAMO_TMP'
        vSchema = 'INTEGRACOESMV'
        # Definir tipos de dados para as colunas
        column_types = {
            'chavebeneficiarioplanosaude': types.Integer(),
            'codigo': types.VARCHAR(180),
            'nk_populacao_mamo': types.VARCHAR(180),
            'carteirinha ': types.VARCHAR(100),
            'nome ': types.VARCHAR(180),
            'cpf': types.VARCHAR(100),
            'tipo_dependencia': types.VARCHAR(180),
            'parentesco': types.VARCHAR(180),
            'idade': types.Integer(),
            'faixa_etaria': types.VARCHAR(50),
            'sexo ': types.VARCHAR(50),
            'cidade': types.VARCHAR(150),
            'cep': types.VARCHAR(30),
            'bairro': types.VARCHAR(50),
            'celular ': types.VARCHAR(50),
            'telefone': types.VARCHAR(50),
            'email': types.VARCHAR(180),
            'comp_ini': types.VARCHAR(180),
            'comp_fim': types.VARCHAR(180),
            'status_ans': types.VARCHAR(50),
            'status_unimed': types.VARCHAR(50),
            'titular_nome': types.VARCHAR(180),
            'grupoempresa': types.VARCHAR(150),
            'tipoempresa_detalhado': types.VARCHAR(150),
            'contratocodigo': types.VARCHAR(150),
            'contratocodajustado': types.VARCHAR(150),
            'contratonome': types.VARCHAR(180),
            'codigoproduto': types.VARCHAR(150),
            'nomeproduto': types.VARCHAR(180),
            'grupoempresa_mv': types.VARCHAR(180),
            'compreferencia':  types.Integer(),
            'inicio_vigencia': types.VARCHAR(15),
            'fim_vigencia': types.VARCHAR(15)          
            
        }

        # Tamanho do lote (número de registros a serem inseridos de cada vez)
        batch_size = 500

        # Construa a consulta SQL para excluir todos os dados da tabela
        delete_query = text(f'DELETE FROM {vSchema}.{table_name} WHERE 1=1 AND COMPREFERENCIA = {vComp}')
        conn = engine.connect()
        
        result = conn.execute(delete_query)

        conn.commit()


        # Inserindo os dados na tabela com tipos de dados explicitamente definidos
        for i in range(0, len(vDf), batch_size):
            vCount = i
            df_batch = vDf[i:i+batch_size]
            df_batch.to_sql(table_name, con=conn, if_exists='append', index=False, schema=vSchema, dtype=column_types)
            del df_batch  # Liberar memória
            conn.commit()  # Realizar commit após cada inserção em lote
    
    finally:
        # Fechar a conexão
        if conn:
            conn.close()
            print(f"       >> Conexão com o banco de dados encerrada...<<")
        if engine:
            engine.dispose()

def popularTbleMamografiaTmp():
    # Data atual
    data_atual = datetime.date.today()

    # Primeiro dia do mês para a data inicial
    data_inicial = data_atual.replace(day=1)

    # Último dia do mês para a data final
    prox_mes = data_atual.replace(month=data_atual.month+1, day=1)
    data_final = prox_mes - datetime.timedelta(days=1)

    # Definindo a lista para armazenar os DataFrames de cada consulta
    dfs = []

    # Defina seus parâmetros aqui
    vDia = '1'

    try:
        # Conectar ao banco de dados
        conn = conectDWUnimed()
        if conn is None:
            raise Exception("Falha na conexão com o banco de dados")
        # Criando o intervalo de datas
        # Criando o intervalo de datas
        data_atual = data_inicial
        while data_atual <= data_final:
            # Ajustando a DATA_REFERENCIA para o último dia de cada mês
            proximo_mes = data_atual.replace(day=28) + datetime.timedelta(days=4)
            DATA_REFERENCIA = proximo_mes - datetime.timedelta(days=proximo_mes.day)
            COMP = DATA_REFERENCIA.strftime('%Y%m')

            #print(DATA_REFERENCIA.strftime('%Y-%m-%d')+' - '+COMP)
            
            # Seu script com os parâmetros substituídos
            script = f"""with tbBenf as (SELECT distinct L.chavebeneficiarioplanosaude,
                            '{COMP}'  AS COMP,
                            --CONVERT(VARCHAR(6), GETDATE(), 112) AS COMP,
                            fim_comp,
                            
                            L.NUMEROCARTAO AS CARTEIRINHA,
                            L.[%Beneficiario],
                            L.contratantechave
                            from raw.stgLinkTable L
                            inner JOIN DWUNIMED.dmartSinist.dimBeneficiario b ON (l.chavebeneficiarioplanosaude = b.chave)
                            where L.classe_beneficiario = -1895935845
                            and L.tipo_empresa <> 'INTERCAMBIO'
                            and L.tipo_empresa not in ('SOU')
                            and '{COMP}'  between inicio_comp and fim_comp
                            --and CONVERT(VARCHAR(6), GETDATE(), 112) between inicio_comp and fim_comp
                            AND b.SEXO = 'FEMININO'
                            AND (DATEDIFF(YEAR, b.dtnascimento, CAST('{DATA_REFERENCIA}' AS DATE)) -  
                    CASE
                        WHEN MONTH(b.dtnascimento) > MONTH(CAST('{DATA_REFERENCIA}' AS DATE)) OR
                            (MONTH(b.dtnascimento) = MONTH(CAST('{DATA_REFERENCIA}' AS DATE)) AND
                                DAY(b.dtnascimento) > DAY(CAST('{DATA_REFERENCIA}' AS DATE)))
                        THEN 1
                        ELSE 0
                    END) >= 50
            AND (DATEDIFF(YEAR, b.dtnascimento, CAST('{DATA_REFERENCIA}' AS DATE)) -  
                    CASE
                        WHEN MONTH(b.dtnascimento) > MONTH(CAST('{DATA_REFERENCIA}' AS DATE)) OR
                            (MONTH(b.dtnascimento) = MONTH(CAST('{DATA_REFERENCIA}' AS DATE)) AND
                                DAY(b.dtnascimento) > DAY(CAST('{DATA_REFERENCIA}' AS DATE)))
                        THEN 1
                        ELSE 0
                    END) < 70
                    UNION
                        SELECT distinct L.chavebeneficiarioplanosaude,
                        '{COMP}'  AS COMP,
                        --CONVERT(VARCHAR(6), GETDATE(), 112) AS COMP,		
                        fim_comp,
                        L.NUMEROCARTAO AS CARTEIRINHA,
                        L.[%Beneficiario],
                        L.contratantechave
                        from raw.stgLinkTable l
                        inner JOIN DWUNIMED.dmartSinist.dimBeneficiario b ON (l.chavebeneficiarioplanosaude = b.chave)
                        LEFT JOIN dmartSinist.dimPessoaContrato pc1 on pc1.NK_PESSOACONTRATO = b.NK_PESSOACONTRATO
                        LEFT JOIN dmartSinist.dimContrato C1 ON (c1.nk_contrato = pc1.nk_contrato)
                        where l.classe_beneficiario = -1895935845
                        --and l.tipo_empresa <> 'INTERCAMBIO'
                        --and l.tipo_empresa not in ('SOU')
                        --and 202407 between l.inicio_comp and l.fim_comp
                        and '{COMP}'  between inicio_comp and fim_comp
                        AND b.SEXO = 'FEMININO'
                        AND pc1.CONTRATO_CODIGO = '7830'
                        AND (DATEDIFF(YEAR, b.dtnascimento, CAST('{DATA_REFERENCIA}' AS DATE)) -  
                        CASE
                        WHEN MONTH(b.dtnascimento) > MONTH(CAST('{DATA_REFERENCIA}' AS DATE)) OR
                            (MONTH(b.dtnascimento) = MONTH(CAST('{DATA_REFERENCIA}' AS DATE)) AND
                                DAY(b.dtnascimento) > DAY(CAST('{DATA_REFERENCIA}' AS DATE)))
                        THEN 1
                        ELSE 0
                    END) >= 50
                        AND (DATEDIFF(YEAR, b.dtnascimento, CAST('{DATA_REFERENCIA}' AS DATE)) -  
                    CASE
                        WHEN MONTH(b.dtnascimento) > MONTH(CAST('{DATA_REFERENCIA}' AS DATE)) OR
                            (MONTH(b.dtnascimento) = MONTH(CAST('{DATA_REFERENCIA}' AS DATE)) AND
                                DAY(b.dtnascimento) > DAY(CAST('{DATA_REFERENCIA}' AS DATE)))
                        THEN 1
                        ELSE 0
                    END) < 70
                ), pContrato as (
                SELECT DISTINCT pc1.nk_pessoacontrato,
                COALESCE(pc1.GRUPO_CONTRATO_CODIGO,pc1.CONTRATO_NOME) AS grupo_contrato_nome,
                c1.contrato_codigo,
                c1.contrato_codigo_ajustado,
                pc1.tipo_empresa_detalhado_qlik,
                pc1.contrato_nome,
                pc1.nome_produto,
                pc1.titular_codigo,
                pc1.titular_nome,
                pc1.titular_cnp,
                pc1.codigo_produto
                from dmartSinist.dimPessoaContrato pc1
                LEFT JOIN dmartSinist.dimContrato C1 ON (c1.nk_contrato = pc1.nk_contrato)
                where 1=1
                )
                select distinct BEF.CHAVEBENEFICIARIOPLANOSAUDE,
                b.CODIGO,
                                CONCAT(b.CODIGO,bef.COMP) AS NK_POPULACAO_MAMO,
                                bef.CARTEIRINHA,
                                b.NOME,
                                b.CNP,
                                b.tipo_dependencia,
                                b.parentesco,
                                b.IDADE,
                                b.FAIXA_ETARIA,
                                b.SEXO,
                                b.cidade,
                                b.cep,
                                b.bairro,
                                B.CELULAR,
                                B.TELEFONE,
                                b.email,
                                B.INICIO_VIGENCIA,
                                B.FIM_VIGENCIA,
                                FORMAT(CAST(B.INICIO_VIGENCIA AS DATE), 'yyyyMM') AS COMP_INI, 
                                FORMAT(CAST(B.FIM_VIGENCIA AS DATE), 'yyyyMM') AS COMP_FIM,
                                b.STATUS_ANS,
                                b.STATUS_UNIMED,
                            pc.titular_nome,
                                coalesce(B.APS,'NAO') AS APS,
                                COALESCE(B.NOME_APS,'NAO IDENTIFICADO') AS NOME_APS, 
                                pc.grupo_contrato_nome as grupoempresa,
                                pc.tipo_empresa_detalhado_qlik as tipo_empresa,
                                pc.contrato_codigo,
                                pc.contrato_codigo_ajustado,
                                pc.contrato_nome as contratoNome,
                                pc.codigo_produto,
                                pc.nome_produto,
                                CASE
                                    WHEN  pc.contrato_codigo = '1180' THEN
                                        'PRODUTO APS'
                                    WHEN pc.contrato_codigo IN ('2289','80028','21797','21823','2288') THEN
                                        'AJUSTADA'
                                    WHEN  pc.codigo_produto IN ('482947196','482958191','482959190','482960193','487263201','487264209','487265207','487266205') THEN	
                                        'REFERENCIADA'
                                    WHEN  pc.codigo_produto IN ('470617140','470618148','473014143','475135153','482950196','482951194','482952192','482953191','482956195','482957193','482961191','482962190','482963198','482964196','488143215','488169219','488173217','492805229','496521233','496059239','499322245','498701242','498704247','497683235','497684233','497676232','497677231','497675234','497674236') THEN	
                                        'PRODUTO APS'
                                    WHEN  pc.codigo_produto IN ('482948194','482949192','482954199','482955197','482965194','482966192') THEN	
                                        'REFERENCIADA-ESTADUAL'
                                    ELSE
                                        'ABERTA'
                                    END AS GRUPOEMPRESA_MV,
                                    bef.COMP as compReferencia


                from tbBenf bef
                INNER JOIN DWUNIMED.dmartSinist.dimBeneficiario b on bef.CHAVEBENEFICIARIOPLANOSAUDE = b.CHAVE
                LEFT JOIN pContrato PC ON b.NK_PESSOACONTRATO = PC.NK_PESSOACONTRATO""".format(COMP=COMP, DATA_REFERENCIA=DATA_REFERENCIA)
            
            # Executar o script aqui...
            df1 = pd.read_sql(script, conn)
            dfCount1 = df1.shape[0]
            #print("Quantidade de linhas MV:", dfCount1)
            #df1.info()
            #print(df1.info())

            # Remover pontos e hífens da coluna CPF de df1
            df1['CNP'] = df1['CNP'].str.replace(r'[.-]', '', regex=True)
            # Converter a coluna compferencia para o formato de data
            #df1['data_referencia'] = pd.to_datetime(df1['compreferencia'], format='%Y%m')
            # Formatar a nova coluna como 'YYYY-MM-01'
            #df1['data_referencia'] = df1['data_referencia'].dt.strftime('%Y-%m-01')
            
            # Adicionar o DataFrame ao final da lista
            dfs.append(df1)
        
            # Incrementando para o próximo mês
            if data_atual.month == 12:
                # Se o mês atual for dezembro, vá para janeiro do próximo ano
                data_atual = datetime.date(data_atual.year + 1, 1, 1)
            else:
                # Caso contrário, vá para o próximo mês do mesmo ano
                data_atual = datetime.date(data_atual.year, data_atual.month + 1, 1)



            
            #column_names = ['codigo','nk_populacao_mamo','carteirinha','nome','cpf','tipo_dependencia','parentesco','idade','faixa_etaria','sexo','cidade','cep','bairro','celular','telefone','email','comp_ini','comp_fim','status_ans','status_unimed','rede','titular_nome','aps','nome_aps','grupoempresa','tipoempresa_detalhado','contratocodigo','codigoproduto','nomeproduto','grupoempresa_mv','cd_prestador_referencia','medico_familia','cd_multi_empresa','ds_multi_empresa','compreferencia','inicio_vigencia','fim_vigencia']
        replaceColumns = {
            'CHAVEBENEFICIARIOPLANOSAUDE':'chavebeneficiarioplanosaude',
            'CODIGO':'codigo',
            'NK_POPULACAO_MAMO':'nk_populacao_mamo',
            'CARTEIRINHA':'carteirinha',
            'NOME':'nome',
            'CNP':'cpf',
            'tipo_dependencia':'tipo_dependencia',
            'parentesco':'parentesco',
            'IDADE':'idade',
            'FAIXA_ETARIA':'faixa_etaria',
            'SEXO':'sexo',
            'cidade':'cidade',
            'cep':'cep',
            'bairro':'bairro',
            'CELULAR':'celular',
            'TELEFONE':'telefone',
            'email':'email',
            'COMP_INI':'comp_ini',
            'COMP_FIM':'comp_fim',
            'STATUS_ANS':'status_ans',
            'STATUS_UNIMED':'status_unimed',
            'titular_nome':'titular_nome',
            'APS':'aps',
            'NOME_APS':'nome_aps',
            'grupoempresa':'grupoempresa',
            'tipo_empresa':'tipoempresa_detalhado',
            'contrato_codigo':'contratocodigo',
            'contrato_codigo_ajustado':'contratocodajustado',
            'contratoNome':'contratonome',
            'codigo_produto':'codigoproduto',
            'nome_produto':'nomeproduto',
            'GRUPOEMPRESA_MV':'grupoempresa_mv',
            'compReferencia':'compreferencia',
            'INICIO_VIGENCIA':'inicio_vigencia',
            'FIM_VIGENCIA':'fim_vigencia'
        }


        # Concatenar os DataFrames da lista dfs em um único DataFrame
        df_combined = pd.concat(dfs, ignore_index=True)

        df_combined = df_combined.rename(columns=replaceColumns)
        # Salvar o DataFrame em um arquivo Parquet
        df_combined.to_parquet('D://projectPython//schedulersPython//Project_ProcessingMamografia014//storageDB//tbPopulacaoDW.parquet')
        
        try:
            creatTablePopTmp(df_combined,COMP)
        except Exception as e:
            print(f"ERROR NO PROCESSO <=> {e} ...")   
            
        
    except Exception as e:
            logging.error(f'{e} - ERROR FUNCTION popularTbleMamografiaTmp')
            print(f"       >> {e} - ERROR FUNCTION popularTbleMamografiaTmp...<<") 

    finally:
        # Fechar a conexão
        if conn:
            conn.dispose()
            print(f"       >> Conexão com o banco de dados encerrada...<<")   

def creatTablePopulacaoProcess(vDf):
    engine = None
    conn = None
    try:
        engine = connectionMV()
        if engine is None:
            raise Exception("Falha na conexão com o banco de dados")
        
        table_name = 'TB_POPULACAO_MAMOGRAFIA'
        vSchema = 'INTEGRACOESMV'
        # Definir tipos de dados para as colunas
        column_types = {
            'chavebeneficiarioplanosaude': types.Integer(),
            'codigo'                    : types.VARCHAR(180),
            'nk_populacao_mamo'         : types.VARCHAR(180),
            'carteirinha '              : types.VARCHAR(100),
            'nome '                     : types.VARCHAR(180),
            'cpf'                       : types.VARCHAR(100),
            'tipo_dependencia'          : types.VARCHAR(180),
            'parentesco'                : types.VARCHAR(180),
            'idade'                     : types.Integer(),
            'faixa_etaria'              : types.VARCHAR(50),
            'sexo '                     : types.VARCHAR(50),
            'cidade'                    : types.VARCHAR(150),
            'cep'                       : types.VARCHAR(30),
            'bairro'                    : types.VARCHAR(50),
            'celular '                  : types.VARCHAR(50),
            'telefone'                  : types.VARCHAR(50),
            'email'                     : types.VARCHAR(180),
            'comp_ini'                  : types.VARCHAR(180),
            'comp_fim'                  : types.VARCHAR(180),
            'status_ans'                : types.VARCHAR(50),
            'status_unimed'             : types.VARCHAR(50),
            'titular_nome'              : types.VARCHAR(180),
            'grupoempresa'              : types.VARCHAR(150),
            'tipoempresa_detalhado'     : types.VARCHAR(150),
            'contratocodigo'            : types.VARCHAR(150),
            'contratocodajustado'       : types.VARCHAR(150),
            'contratonome'              : types.VARCHAR(150),
            'codigoproduto'             : types.VARCHAR(150),
            'nomeproduto'               : types.VARCHAR(150),
            'grupoempresa_mv'           : types.VARCHAR(150),
            'ds_especialidade'          : types.VARCHAR(130),
            'compreferencia '           : types.Integer(),
            'origem'                    : types.VARCHAR(150),
            'dt_criacao'                : types.VARCHAR(150),
            'inicio_vigencia'           : types.VARCHAR(150),
            'fim_vigencia'              : types.VARCHAR(150)          
            
        }

        # Tamanho do lote (número de registros a serem inseridos de cada vez)
        batch_size = 500

        # Construa a consulta SQL para excluir todos os dados da tabela
        delete_query = text(f'TRUNCATE TABLE {vSchema}.{table_name}')

        # Execute a consulta para excluir todos os dados da tabela
        conn = engine.connect()
        conn.execute(delete_query)
        
        # Inserindo os dados na tabela com tipos de dados explicitamente definidos
        for i in range(0, len(vDf), batch_size):
            vCount = i
            df_batch = vDf[i:i+batch_size]
            df_batch.to_sql(table_name, con=conn, if_exists='append', index=False, schema=vSchema, dtype=column_types)
            del df_batch  # Liberar memória
            conn.commit()  # Realizar commit após cada inserção em lote
    
    except Exception as e:
        logging.error(f'{e} - ERROR FUNCTION creatTablePopulacaoProcess')
        print(f"       >> {e} - ERROR FUNCTION creatTablePopulacaoProcess...<<") 
        
        
    finally:
        # Fechar a conexão
        if conn:
            conn.close()
            print(f"       >> Conexão com o banco de dados encerrada...<<") 
        if engine:
            engine.dispose()

def creatTableGuiasProcess(vDf):
    engine = None
    conn = None
    try:
        engine = connectionMV()
        if engine is None:
            raise Exception("Falha na conexão com o banco de dados")
        
        table_name = 'TB_GUIAS_MAMOGRAFIA'
        vSchema = 'INTEGRACOESMV'
        # Definir tipos de dados para as colunas
        column_types = {
            'carteirinha '                  : types.VARCHAR(100),
            'nome '                         : types.VARCHAR(180),
            'cpf'                           : types.VARCHAR(100),
            'idade'                         : types.Integer(),
            'faixa_etaria'                  : types.VARCHAR(50),
            'sexo '                         : types.VARCHAR(50),
            'cidade'                        : types.VARCHAR(150),
            'bairro'                        : types.VARCHAR(50),
            'celular '                      : types.VARCHAR(50),
            'telefone'                      : types.VARCHAR(50),
            'email'                         : types.VARCHAR(180),
            'status_ans'                    : types.VARCHAR(50),
            'status_unimed'                 : types.VARCHAR(50),
            'rede '                         : types.VARCHAR(50),
            'aps'                           : types.VARCHAR(50),
            'nome_aps'                      : types.VARCHAR(50),
            'grupoempresa'                  : types.VARCHAR(150),
            'grupoempresa_mv'               : types.VARCHAR(50),
            'cd_prestador_referencia '      : types.VARCHAR(50),
            'medico_familia '               : types.VARCHAR(250),
            'cd_multi_empresa'              : types.VARCHAR(50),
            'ds_multi_empresa'              : types.VARCHAR(50),
            'solicitante'                   : types.VARCHAR(150),
            'especialidade_solicid'         : types.VARCHAR(150),
            'executante'                    : types.VARCHAR(150),
            'nmclasse_credenciado'          : types.VARCHAR(150),
            'classe_credenciado'            : types.VARCHAR(150),
            'classe_executante'             : types.VARCHAR(150),
            'senha_atendimento'             : types.VARCHAR(150),
            'codigo_procedimento'           : types.VARCHAR(150),
            'nome_proceidmento'             : types.VARCHAR(150),
            'dt_solicitacao'                : types.VARCHAR(50),
            'dt_realizacao'                 : types.VARCHAR(50),
            'compreferencia'                : types.Integer(),
            'data_referencia'               : types.VARCHAR(50),
            'dadosMamografia'               :  types.Integer()
            
            
        }

        # Tamanho do lote (número de registros a serem inseridos de cada vez)
        batch_size = 500

        # Construa a consulta SQL para excluir todos os dados da tabela
        delete_query = text(f'TRUNCATE TABLE {vSchema}.{table_name}')

        # Execute a consulta para excluir todos os dados da tabela
        conn = engine.connect()
        conn.execute(delete_query)
        
        # Inserindo os dados na tabela com tipos de dados explicitamente definidos
        for i in range(0, len(vDf), batch_size):
            vCount = i
            df_batch = vDf[i:i+batch_size]
            df_batch.to_sql(table_name, con=conn, if_exists='append', index=False, schema=vSchema, dtype=column_types)
            del df_batch  # Liberar memória
            conn.commit()  # Realizar commit após cada inserção em lote
    
    except Exception as e:
        logging.error(f'{e} - ERROR FUNCTION creatTableGuiasProcess')
        print(f"       >> {e} - ERROR FUNCTION creatTableGuiasProcess...<<") 
        
        
    finally:
        # Fechar a conexão
        if conn:
            conn.close()
            print(f"       >> Conexão com o banco de dados encerrada...<<") 
            
        if engine:
            engine.dispose()

def creatTableMamografiaProcess(vDf):
    engine = None
    conn = None
    try:
        engine = connectionMV()
        if engine is None:
            raise Exception("Falha na conexão com o banco de dados")
        
        table_name = 'TB_MAMOGRAFIA_STATUS'
        vSchema = 'INTEGRACOESMV'
        # Definir tipos de dados para as colunas
        column_types = {
            'chavebeneficiarioplanosaude'   : types.Integer(),
            'carteirinha'                   : types.VARCHAR(100),
            'em_dia'                        : types.Integer(),
            'em_atraso'                     : types.Integer(),
            'sem_exames'                    : types.Integer(),
            'compreferencia'                : types.Integer()
        }

        # Tamanho do lote (número de registros a serem inseridos de cada vez)
        batch_size = 500

        # Construa a consulta SQL para excluir todos os dados da tabela
        delete_query = text(f'TRUNCATE TABLE {vSchema}.{table_name}')

        # Execute a consulta para excluir todos os dados da tabela
        conn = engine.connect()
        conn.execute(delete_query)
        
        # Inserindo os dados na tabela com tipos de dados explicitamente definidos
        for i in range(0, len(vDf), batch_size):
            vCount = i
            df_batch = vDf[i:i+batch_size]
            df_batch.to_sql(table_name, con=conn, if_exists='append', index=False, schema=vSchema, dtype=column_types)
            del df_batch  # Liberar memória
            conn.commit()  # Realizar commit após cada inserção em lote
    
    except Exception as e:
        logging.error(f'{e} - ERROR FUNCTION creatTableMamografiaProcess')
        print(f"       >> {e} - ERROR FUNCTION creatTableMamografiaProcess...<<")
        
        
    finally:
        # Fechar a conexão
        if conn:
            conn.close()
            print(f"       >> Conexão com o banco de dados encerrada...<<")
        if engine:
            engine.dispose()             

def etlPopulacaoMamo():
    """BASE DE DADOS MV"""
    # Criação do DataFrame
    try:
        # Conectar ao banco de dados
        conn = connectionMV()
        if conn is None:
            raise Exception("Falha na conexão com o banco de dados")
        
        #SCRIPT MV
        query = """WITH tbHrp as (
                                select distinct hrp.benf_cpf,
                                    codigo_paciente,
                                    hrp.cd_prestador_referencia,
                                    hrp.medico_familia, 
                                    hrp.ds_especialidade,    
                                    hrp.cd_multi_empresa,
                                    hrp.ds_multi_empresa
                                    
                                from TB_BENEFICIARIO_MV_DEP_HRP hrp 
                                where 1=1
                                --and codigo_paciente = 23287
                                ), tbStatusBenf as (
                                select distinct hrp.codigo_paciente,
                                                hrp.benf_cpf,
                                    hrp.benf_status_unimed
                                from TB_BENEFICIARIO_MV_DEP_HRP hrp 
                                where 1=1
                                --and hrp.codigo_paciente = 23287
                                and hrp.benf_carteirinha = (select max(benf_carteirinha) from TB_BENEFICIARIO_MV_DEP_HRP hrp2 where 1=1 and hrp.codigo_paciente = hrp2.codigo_paciente)
                                )SELECT
                                DISTINCT  mam.chavebeneficiarioplanosaude,
                                    mam.CODIGO,
                                    hrp.codigo_paciente,
                                    mam.NK_POPULACAO_MAMO,
                                    mam.CARTEIRINHA,
                                    mam.NOME,
                                    mam.CPF,
                                    mam.tipo_dependencia,
                                    mam.parentesco,
                                    mam.IDADE,
                                    mam.FAIXA_ETARIA,
                                    mam.SEXO,
                                    mam.cidade,
                                    mam.cep,
                                    mam.bairro,
                                    mam.CELULAR,
                                    mam.TELEFONE,
                                    mam.email,
                                    mam.inicio_vigencia,
                                    mam.fim_vigencia,
                                    mam.COMP_INI, 
                                    mam.COMP_FIM,
                                    mam.STATUS_ANS,
                                    mam.STATUS_UNIMED,
                                    mam.titular_nome,
                                    mam.APS,
                                    mam.NOME_APS, 
                                    mam.grupoempresa,
                                    mam.tipoempresa_detalhado,
                                    mam.contratocodigo,
                                    mam.contratocodajustado,
                                    mam.contratonome,
                                    mam.codigoproduto,
                                    mam.nomeproduto,
                                    mam.grupoempresa_mv,
                                    mam.compreferencia,
                                    CASE 
                                            WHEN  grupoempresa_mv <>  'PRODUTO APS' AND SUBSTR(TRIM(REPLACE(CARTEIRINHA,' ','')),0,4) <> '0014' THEN
                                            'INTERCAMBIO'
                                            ELSE
                                            'APS'
                                        END AS REDE,
                                    nvl(hrp.cd_prestador_referencia,'-1') as cd_prestador_referencia,
                                    nvl(hrp.medico_familia,'NAO IDENTIFICADO') AS medico_familia,     
                                    nvl(hrp.ds_especialidade,'NAO IDENTIFICADO') AS ds_especialidade,  
                                    NVL(hrp.cd_multi_empresa,'-1') AS cd_multi_empresa,
                                    NVL(hrp.ds_multi_empresa,'NAO IDENTIFICADO') AS ds_multi_empresa,
                                    NVL(status.benf_status_unimed,'NAO IDENTIFICADO') AS benf_status_unimed
                                        
                                    FROM TMP_TB_POPULACAO_MAMO_tmp mam
                                    LEFT JOIN tbHrp hrp ON replace(replace(mam.cpf,'.',''),'-','') = hrp.benf_cpf
                                    LEFT JOIN tbStatusBenf status ON hrp.benf_cpf = status.benf_cpf
                                    --LEFT JOIN TB_BENEFICIARIOS_MV_DEP_HRP hrp ON replace(replace(mam.cpf,'.',''),'-','') = hrp.benf_cpf
                                    WHERE 1=1
                                    --AND mam.compreferencia = '202301'--(SELECT TO_CHAR(SYSDATE,'YYYYMM') FROM DUAL)
                                    --AND GRUPOEMPRESA_MV = 'PRODUTO APS'
                                    --AND   mam.CODIGO = '0000057717'-- CART 00145080164096512
                                    --and  mam.CARTEIRINHA = '00142679295668003'
                                    ORDER BY 5
                                    """;

        df1 = pd.read_sql(query, conn)
        dfCount1 = df1.shape[0]
        #print("Quantidade de linhas MV:", dfCount1)
        print(f"       >> Quantidade de linhas MV: << {dfCount1} ")
        #print(df1.info())

        # Remover pontos e hífens da coluna CPF de df1
        df1['cpf'] = df1['cpf'].str.replace(r'[.-]', '', regex=True)
        # Converter a coluna compferencia para o formato de data
        df1['data_referencia'] = pd.to_datetime(df1['compreferencia'], format='%Y%m')
        # Formatar a nova coluna como 'YYYY-MM-01'
        df1['data_referencia'] = df1['data_referencia'].dt.strftime('%Y-%m-01')
        # Salvar o DataFrame em um arquivo Parquet
        df1.to_parquet('D://projectPython//schedulersPython//Project_ProcessingMamografia014//storageDB//tbPopulacao.parquet')
        
        column_names = ['chavebeneficiarioplanosaude','codigo','nk_populacao_mamo','carteirinha','nome','cpf','tipo_dependencia','parentesco','idade','faixa_etaria','sexo','cidade','cep','bairro','celular','telefone','email','comp_ini','comp_fim','status_ans','status_unimed','rede','titular_nome','aps','nome_aps','grupoempresa','tipoempresa_detalhado','contratocodigo','codigoproduto','nomeproduto','grupoempresa_mv','cd_prestador_referencia','medico_familia','ds_especialidade','cd_multi_empresa','ds_multi_empresa','compreferencia','inicio_vigencia','fim_vigencia']

        #column_names = ['codigo_paciente','benf_cpf','benf_codigo_hrp','nr_identidade','benf_carteirinha','beneficiario','orgao_emissor_identidade','data_nascimento','idade','genero','descricao_cor_pele','estado_civil','nome_mae','nome_pai','telefone','celular','cep','descricao_email','benf_cidade','bairro','logradouro','nr_logradouro','complemento','tp_situacao','ds_observacao','ds_grau_ins','dt_cadastro_manual','dt_atualizacao','nm_usuario_ultima_atualizacao','benf_inicio_contrato','benf_fim_contrato','cd_prestador_referencia','prestador','medico_familia','benf_aps','benf_nome_aps','passou_pelo_cias','cd_multi_empresa','ds_multi_empresa','benf_status_ans','benf_status_unimed','grupo_empresa_hrp','tipo_empresa_hrp','contrato_codigo_hrp','contrato_codigo_ajust_hrp','contrato_nome_hrp','codigo_produto_hrp','nome_produto_hrp']

        # Criar um DataFrame a partir do resultado da consulta
        df_resultPopMamo = pd.DataFrame(df1, columns=column_names)
        
        try:
            logging.info(f' CRIANDO A TABLE TB_POPULACAO_MAMOGRAFIA ')
            print(f"       >> CRIANDO A TABLE TB_POPULACAO_MAMOGRAFIA << ")
            
            creatTablePopulacaoProcess(df_resultPopMamo)
            
            logging.info(f' CRIAÇÃO DA TABLE TB_POPULACAO_MAMOGRAFIA COM SUCESSO... ')
            print(f"       >> CRIACAO DA TABLE TB_POPULACAO_MAMOGRAFIA COM SUCESSO << ")
        except Exception as e:
            logging.error(f'{e} - ERROR FUNCTION TB_POPULACAO_MAMOGRAFIA')
            print(f"       >> ERROR NA CRIAÇÃO DA TABLE TB_POPULACAO_MAMOGRAFIA ...  <=> {e} ...")   

    except Exception as e:
        logging.error(f'{e} - ERROR FUNCTION TB_POPULACAO_MAMOGRAFIA')
        print(f"       >> {e} - ERROR FUNCTION TB_POPULACAO_MAMOGRAFIA")     
        

    finally:
        # Fechar a conexão
        if conn:
            conn.dispose()
            print(f"       >> Conexão com o banco de dados encerrada. << ")   
            
def etlGuiasMamo():
    """BASE DE DADOS MV"""
    # Criação do DataFrame
    try:
        # Conectar ao banco de dados
        conn = connectionMV()
        if conn is None:
            raise Exception("Falha na conexão com o banco de dados")
        
        #SCRIPT MV
        query = """SELECT DISTINCT  NK_GUIA ,
                                            CHAVE          , 
                                        CODIGO            ,
                                        BENEFICIARIO           ,
                                        CPF_BENEFICIARIO        ,
                                        SEXO                     ,
                                        IDADE_BENEFICIARIO       ,
                                        DATA_NASCIMENTO          ,
                                        CARTEIRINHA              ,
                                        BENF_CELULAR             ,
                                        BENF_TELEFONE            , 
                                        EXECUTANTE_NOME          ,
                                        ESPECIALIDADE_EXEC       ,
                                        SOLICITANTE_NM            ,
                                        ESPECIALIDADE_SOLID      ,
                                        CREDENCIADO_NOME AS NOME_CREDENCIADO,
                                        CREDENCIADO_NMCLASSE AS NMCLASSE_CREDENCIADO,
                                        CREDENCIADO_CLASSE AS CLASSE_CREDENCIADO ,
                                        CREDENCIADO_ESPEC AS ESPECIALIDADE_CREDENCIADO,
                                        SENHA_ATENDIMENTO        ,
                                        DATA_SOLICITACAO          ,
                                        DATA_REALIZACAO         ,
                                        CODIGO_PROCEDIMENTO      ,
                                        NOME_PROCEDIMENTO         ,
                                        SCOMPETENCIA             ,
                                        GRUPOEMPRESA             ,
                                            CONTRATONOME          ,
                                            NOMEPRODUTO           ,
                                            NOMEAPS                  
                                FROM TMP_TB_EXTRACT_MAMO p
                                WHERE 1=1
                                ORDER BY SCOMPETENCIA """;

        df2 = pd.read_sql(query, conn)
        dfCount2 = df2.shape[0]
        #print("Quantidade de linhas MV:", dfCount2)
        print(f"       >> Quantidade de linhas MV <<  {dfCount2}")
        # Converter a coluna CODIGO para int64
        df2['codigo'] = pd.to_numeric(df2['codigo'], errors='coerce').astype('Int64')
        df2.to_parquet('D://projectPython//schedulersPython//Project_ProcessingMamografia014//storageDB//tbGuias.parquet')

    except Exception as e:
        logging.error(f'{e} - ERROR FUNCTION etlGuiasMamo')
        print(f"       >> {e} - ERROR FUNCTION etlGuiasMamo") 
        
    finally:
        # Fechar a conexão
        if conn:
            conn.dispose()
            print(f"       >> Conexão com o banco de dados encerrada. << ")   

def connectionDuckDB():
    try:
        #con = duckdb.connect(database=':memory:')
        con = duckdb.connect(database='D://projectPython//schedulersPython//Project_ProcessingMamografia014//storageDB//tbProcessingMamo.db')
        
        return con
    
    except Exception as e:
        logging.error(f'{e} - ERROR FUNCTION startDuckDB')
        print(f"       >> {e} - ERROR FUNCTION startDuckDB...<<") 
        
def createTableProcessing():
    vstart = connectionDuckDB()
    # Execute uma consulta para obter todas as tabelas
    result = vstart.execute("SELECT name FROM sqlite_master WHERE type='table';")

    # Recupere todas as tabelas em uma lista
    tables = result.fetchall()

    # Iterar sobre a lista de tabelas e dropar cada uma
    for row in tables:
        table_name = row[0]
        vstart.execute(f"DROP TABLE IF EXISTS {table_name};")

    print(f"       >> Todas as tabelas foram dropadas com sucesso.")

    # Ler o arquivo .parquet para uma tabela no banco de dados
    vstart.execute("CREATE TABLE tbPopulacao AS SELECT * FROM parquet_scan('D://projectPython//schedulersPython//Project_ProcessingMamografia014//storageDB//tbPopulacao.parquet')")
    vstart.execute("CREATE TABLE tbGuias AS SELECT * FROM parquet_scan('D://projectPython//schedulersPython//Project_ProcessingMamografia014//storageDB//tbGuias.parquet')")

def processingStatusClinic():
    
    try:
        #COMECA AQUI
        con = connectionDuckDB()
        result = con.execute("""WITH tbGuiasMamografia AS (
                                SELECT DISTINCT NK_GUIA, CHAVE, 
                                    CODIGO, 
                                    BENEFICIARIO, CPF_BENEFICIARIO, SEXO, IDADE_BENEFICIARIO, 
                                    DATA_NASCIMENTO, CARTEIRINHA, BENF_CELULAR, BENF_TELEFONE, 
                                    EXECUTANTE_NOME, ESPECIALIDADE_EXEC, SOLICITANTE_NM, 
                                    ESPECIALIDADE_SOLID, NOME_CREDENCIADO, NMCLASSE_CREDENCIADO, 
                                    CLASSE_CREDENCIADO, ESPECIALIDADE_CREDENCIADO, SENHA_ATENDIMENTO, 
                                    DATA_SOLICITACAO, DATA_REALIZACAO, CODIGO_PROCEDIMENTO, 
                                    NOME_PROCEDIMENTO, SCOMPETENCIA, GRUPOEMPRESA, CONTRATONOME, 
                                    NOMEPRODUTO, NOMEAPS
                                FROM tbprocessingmamo.main.tbGuias
                                WHERE 1=1
                                ORDER BY SCOMPETENCIA
                            ), tbValid AS (
                                SELECT DISTINCT chavebeneficiarioplanosaude,
                                CARTEIRINHA, 
                                CASE 
                                WHEN current_date_yyyymm() = COMPREFERENCIA THEN '1'
                                ELSE '2'
                                END AS vValid  
                                FROM tbprocessingmamo.main.tbPopulacao
                                WHERE 1=1
                            ), tbCombined AS (
                                SELECT DISTINCT 
                                    pop.chavebeneficiarioplanosaude,
                                    pop.CARTEIRINHA,
                                    pop.compreferencia,
                                    guias.beneficiario,
                                    CASE 
                                        WHEN vlid.vValid = '1' THEN
                                            CASE 
                                                WHEN date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) >=31 THEN 'SEM EXAMES'
                                                WHEN date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) < 0 THEN 'SEM EXAMES'                   
                                                WHEN date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) >=25 AND date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) <= 30 THEN 'EM ATRASO'
                                                WHEN date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) >=1 AND date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) <25 THEN 'EM DIA'
                                                ELSE 'SEM EXAMES'
                                            END 
                                        ELSE 
                                            CASE 
                                                WHEN date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) >=31 THEN 'SEM EXAMES'
                                                WHEN date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) < 0 THEN 'SEM EXAMES'
                                                WHEN date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) = 0 THEN 'EM DIA'
                                                WHEN date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) >=25 AND date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) <= 30 THEN 'EM ATRASO'
                                                WHEN date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) >=1 AND date_diff('month', CAST(guias.data_realizacao AS DATE), CAST(pop.data_referencia AS DATE)) <25 THEN 'EM DIA'
                                                ELSE 'SEM EXAMES'
                                            END
                                    END	AS Validacao
                                FROM tbprocessingmamo.main.tbPopulacao pop
                                LEFT JOIN tbGuiasMamografia guias ON pop.CPF = REPLACE(REPLACE(guias.CPF_BENEFICIARIO, '.', ''), '-', '')
                                INNER JOIN tbValid vlid ON vlid.CARTEIRINHA = pop.CARTEIRINHA 
                                WHERE 1=1
                                ORDER BY pop.CARTEIRINHA
                            ), tbFinal AS (
                                SELECT 
                                    DISTINCT chavebeneficiarioplanosaude,
                                    CARTEIRINHA,
                                    MAX(CASE WHEN validacao = 'EM DIA' THEN 1 ELSE 0 END) AS EM_DIA,
                                    MAX(CASE WHEN validacao = 'EM ATRASO' THEN 1 ELSE 0 END) AS EM_ATRASO,
                                    MAX(CASE WHEN validacao = 'SEM EXAMES' THEN 1 ELSE 0 END) AS SEM_EXAMES,
                                    compreferencia,
                                FROM tbCombined
                                GROUP BY chavebeneficiarioplanosaude,CARTEIRINHA,compreferencia
                            )
                            SELECT 
                                chavebeneficiarioplanosaude,
                                CARTEIRINHA,
                                CASE
                                    WHEN EM_DIA = 1 THEN 1
                                    ELSE 0
                                END AS EM_DIA,
                                CASE
                                    WHEN EM_DIA = 1 THEN 0
                                    WHEN EM_ATRASO = 1 AND EM_DIA = 0 THEN 1
                                    ELSE 0
                                END AS EM_ATRASO,
                                CASE
                                    WHEN EM_DIA = 0 AND EM_ATRASO = 0 THEN 1
                                    ELSE 0
                                END AS SEM_EXAMES,
                                compreferencia
                            FROM tbFinal
                            ORDER BY CARTEIRINHA;
                            """).fetchall()

        # Nomes das colunas
        column_names = ['chavebeneficiarioplanosaude','carteirinha','em_dia','em_atraso','sem_exames','compreferencia']

        #column_names = ['codigo_paciente','benf_cpf','benf_codigo_hrp','nr_identidade','benf_carteirinha','beneficiario','orgao_emissor_identidade','data_nascimento','idade','genero','descricao_cor_pele','estado_civil','nome_mae','nome_pai','telefone','celular','cep','descricao_email','benf_cidade','bairro','logradouro','nr_logradouro','complemento','tp_situacao','ds_observacao','ds_grau_ins','dt_cadastro_manual','dt_atualizacao','nm_usuario_ultima_atualizacao','benf_inicio_contrato','benf_fim_contrato','cd_prestador_referencia','prestador','medico_familia','benf_aps','benf_nome_aps','passou_pelo_cias','cd_multi_empresa','ds_multi_empresa','benf_status_ans','benf_status_unimed','grupo_empresa_hrp','tipo_empresa_hrp','contrato_codigo_hrp','contrato_codigo_ajust_hrp','contrato_nome_hrp','codigo_produto_hrp','nome_produto_hrp']

        # Criar um DataFrame a partir do resultado da consulta
        df_resultDif = pd.DataFrame(result, columns=column_names)
        try:
            logging.info(f' CRIANDO A TABLE TB_MAMOGRAFIA_STATUS ')
            print(f"       >> Criando a table tb_mamografia_status <<")
            
            creatTableMamografiaProcess(df_resultDif)

            print(f"       >> tb_mamografia_status criada com sucess...<<")
            logging.info(f' TABLE TB_MAMOGRAFIA_STATUS CRIADA COM SUCESS... ')
        except Exception as e:
                logging.error(f'{e} - ERROR TABLE TB_MAMOGRAFIA_STATUS CRIADA')
                print(f"       >> ERROR NO PROCESSO <=> {e} ...")
                
                        
    except Exception as e:
        logging.error(f'{e} - ERROR FUNCTION processingStatusClinic')
        print(f"       >> {e} - ERROR FUNCTION startDuckDB...<<") 
    
    finally:
            con.close()
            print(f"       >> conexao duckdb fechada...<<")

def processingGuias():
    try:
        #COMECA AQUI
        con = connectionDuckDB()
        result = con.execute("""
            WITH tbGuiasMamografia AS (
                SELECT DISTINCT NK_GUIA, CHAVE, 
                    CODIGO, 
                    BENEFICIARIO, CPF_BENEFICIARIO, SEXO, IDADE_BENEFICIARIO, 
                    DATA_NASCIMENTO, CARTEIRINHA, BENF_CELULAR, BENF_TELEFONE, 
                    EXECUTANTE_NOME, ESPECIALIDADE_EXEC, SOLICITANTE_NM, 
                    ESPECIALIDADE_SOLID, NOME_CREDENCIADO, NMCLASSE_CREDENCIADO, 
                    CLASSE_CREDENCIADO, ESPECIALIDADE_CREDENCIADO, SENHA_ATENDIMENTO, 
                    DATA_SOLICITACAO, DATA_REALIZACAO, CODIGO_PROCEDIMENTO, 
                    NOME_PROCEDIMENTO, SCOMPETENCIA, GRUPOEMPRESA, CONTRATONOME, 
                    NOMEPRODUTO, NOMEAPS
                FROM tbprocessingmamo.main.tbGuias
                WHERE 1=1
                ORDER BY SCOMPETENCIA
                )
                select distinct pop.carteirinha,
                    pop.nome,
                    coalesce(POP.cpf,CPF_BENEFICIARIO) as CPF,
                    pop.idade,
                    pop.faixa_etaria,
                    pop.sexo,
                    pop.cidade,
                    pop.bairro,
                    COALESCE(pop.celular,BENF_CELULAR) AS CELULAR,
                    COALESCE(pop.telefone,BENF_TELEFONE) AS TELEFONE,
                    pop.email,
                    pop.status_ans,
                    pop.status_unimed,
                    pop.rede,
                    pop.aps,
                    pop.nome_aps,
                    pop.grupoempresa,
                    pop.grupoempresa_mv,
                    COALESCE(pop.cd_prestador_referencia,'-1') AS cd_prestador_referencia,
                    COALESCE(pop.medico_familia,'NAO VINCULADO') AS medico_familia,
                    COALESCE(pop.cd_multi_empresa,'-1') CD_MULTI_EMPRESA,
                    COALESCE(pop.ds_multi_empresa,'NAO IDENTIFICADO') AS DS_MULTI_EMPRESA,
                    
                    
                    COALESCE (guias.solicitante_nm,'NAO IDENTIFICADO') AS solicitante,
                    COALESCE (guias.ESPECIALIDADE_SOLID,'NAO IDENTIFICADO') AS ESPECIALIDADE_SOLICID,
                    COALESCE (guias.NOME_CREDENCIADO,'NAO IDENTIFICADO')AS EXECUTANTE,
                    COALESCE (guias.NMCLASSE_CREDENCIADO,'NAO IDENTIFICADO') AS NMCLASSE_CREDENCIADO, 
                    COALESCE (guias.CLASSE_CREDENCIADO,'NAO IDENTIFICADO') AS CLASSE_CREDENCIADO, 
                    COALESCE (guias.ESPECIALIDADE_CREDENCIADO,'NAO IDENTIFICADO') AS CLASSE_EXECUTANTE, 
                    COALESCE (guias.SENHA_ATENDIMENTO,'-1')AS SENHA_ATENDIMENTO,
                    COALESCE (guias.CODIGO_PROCEDIMENTO,'-1')AS CODIGO_PROCEDIMENTO, 
                    COALESCE (guias.NOME_PROCEDIMENTO,'NAO IDENTIFICADO') AS NOME_PROCEDIMENTO,
                    
                    
                    COALESCE (guias.data_solicitacao,'1900-01-01') AS dt_solicitacao,
                    COALESCE (guias.data_realizacao,'1900-01-01') AS dt_realizacao,
                    pop.compreferencia,
                    pop.data_referencia
                    
                from tbprocessingmamo.main.tbPopulacao pop
                LEFT JOIN tbGuiasMamografia guias ON pop.CPF = REPLACE(REPLACE(guias.CPF_BENEFICIARIO, '.', ''), '-', '')
                where 1=1
                --and pop.codigo = '0000057717'
                --and pop.carteirinha = '00141175003988002'
                --and (pop.compreferencia = guias.SCOMPETENCIA OR pop.compreferencia <> guias.SCOMPETENCIA)
                and pop.compreferencia = guias.SCOMPETENCIA
                --AND guias.SCOMPETENCIA IS NULL 
                UNION 
                select distinct pop.carteirinha,
                    pop.nome,
                    coalesce(POP.cpf,CPF_BENEFICIARIO) as CPF,
                    pop.idade,
                    pop.faixa_etaria,
                    pop.sexo,
                    pop.cidade,
                    pop.bairro,
                    COALESCE(pop.celular,BENF_CELULAR) AS CELULAR,
                    COALESCE(pop.telefone,BENF_TELEFONE) AS TELEFONE,
                    pop.email,
                    pop.status_ans,
                    pop.status_unimed,
                    pop.rede,
                    pop.aps,
                    pop.nome_aps,
                    pop.grupoempresa,
                    pop.grupoempresa_mv,
                    COALESCE(pop.cd_prestador_referencia,'-1') AS cd_prestador_referencia,
                    COALESCE(pop.medico_familia,'NAO VINCULADO') AS medico_familia,
                    COALESCE(pop.cd_multi_empresa,'-1') CD_MULTI_EMPRESA,
                    COALESCE(pop.ds_multi_empresa,'NAO IDENTIFICADO') AS DS_MULTI_EMPRESA,
                    
                    
                    COALESCE (guias.solicitante_nm,'NAO IDENTIFICADO') AS solicitante,
                    COALESCE (guias.ESPECIALIDADE_SOLID,'NAO IDENTIFICADO') AS ESPECIALIDADE_SOLICID,
                    COALESCE (guias.NOME_CREDENCIADO,'NAO IDENTIFICADO')AS EXECUTANTE,
                    COALESCE (guias.NMCLASSE_CREDENCIADO,'NAO IDENTIFICADO') AS NMCLASSE_CREDENCIADO, 
                    COALESCE (guias.CLASSE_CREDENCIADO,'NAO IDENTIFICADO') AS CLASSE_CREDENCIADO, 
                    COALESCE (guias.ESPECIALIDADE_CREDENCIADO,'NAO IDENTIFICADO') AS CLASSE_EXECUTANTE, 
                    COALESCE (guias.SENHA_ATENDIMENTO,'-1')AS SENHA_ATENDIMENTO,
                    COALESCE (guias.CODIGO_PROCEDIMENTO,'-1')AS CODIGO_PROCEDIMENTO, 
                    COALESCE (guias.NOME_PROCEDIMENTO,'NAO IDENTIFICADO') AS NOME_PROCEDIMENTO,
                    
                    
                    COALESCE (guias.data_solicitacao,'1900-01-01') AS dt_solicitacao,
                    COALESCE (guias.data_realizacao,'1900-01-01') AS dt_realizacao,
                    pop.compreferencia,
                    pop.data_referencia
                    
                from tbprocessingmamo.main.tbPopulacao pop
                LEFT JOIN tbGuiasMamografia guias ON pop.CPF = REPLACE(REPLACE(guias.CPF_BENEFICIARIO, '.', ''), '-', '')
                where 1=1
                --and pop.codigo = '0000057717'
                --and pop.carteirinha = '00141175003988002'
                --and (pop.compreferencia = guias.SCOMPETENCIA OR pop.compreferencia <> guias.SCOMPETENCIA)
                --and pop.compreferencia = guias.SCOMPETENCIA
                AND guias.SCOMPETENCIA IS NULL 
                
                """).fetchall()
        # Nomes das colunas
        column_names = ['carteirinha','nome','cpf','idade','faixa_etaria','sexo','cidade','bairro','celular','telefone','email','status_ans','status_unimed','rede','aps','nome_aps','grupoempresa','grupoempresa_mv','cd_prestador_referencia','medico_familia','cd_multi_empresa','ds_multi_empresa','solicitante','especialidade_solicid','executante','nmclasse_credenciado','classe_credenciado','classe_executante','senha_atendimento','codigo_procedimento','nome_procedimento','dt_solicitacao','dt_realizacao','compreferencia','data_referencia']

        # Criar um DataFrame a partir do resultado da consulta
        df_resultGuias = pd.DataFrame(result, columns=column_names)
        #creatTableMamografiaProcess(df_resultDif)
        try:
            logging.info(f' CRIANDO A TABLE TB_GUIAS_MAMOGRAFIA ')
            print(f"       >> Criando a table tb_guias_mamografia...<<")

            creatTableGuiasProcess(df_resultGuias)

            logging.info(f' CRIANDO A TABLE TB_GUIAS_MAMOGRAFIA ')
            print(f"       >> tb_guias_mamografia criada com sucess...<<")
        except Exception as e:
            logging.error(f'{e} - ERROR FUNCTION creatTableGuiasProcess')
            print(f"       >> ERROR NO PROCESSO <=> {e} ...<<")
            
    
    except Exception as e:
        logging.error(f'{e} - ERROR FUNCTION processingGuias')
        print(f"       >> ERROR NO PROCESSO <=> {e} ...<<") 

    finally:
            con.close()
            print(f"       >> conexao duckdb fechada...<<")         
            
def main():

    
    
    print(f"       >> PREPARANDO A POPULACAO MAMOGRAFIA >=50 < 70... ")
    logging.info(f"       >> PREPARANDO A POPULACAO MAMOGRAFIA >=50 < 70... ")
    
    etlPopulacaoMamo()
        
    print(f"       >> POPULACAO MAMOGRAFIA CARREGADA... ")
    print(f"       >> *************************************************** << ")
    logging.info(f"       >> POPULACAO MAMOGRAFIA CARREGADA")
    #--------------------------------------------------------------------------------------------
    print(f"       >> PREPARANDO GUIAS MAMOGRAFIA... ")
    logging.info(f"       >> PREPARANDO GUIAS MAMOGRAFIA... ")
    
    etlGuiasMamo()
    
    print(f"       >> GUIAS GUIAS CARREGADA... ")
    print(f"       >> *************************************************** << ")
    logging.info(f"       >> GUIAS GUIAS CARREGADA")
    #--------------------------------------------------------------------------------------------
    print(f"       >> START DUCKDB COM AS BASES DE DADOS... ")
    logging.info(f"       >> PREPARANDO GUIAS MAMOGRAFIA... ")
    
    createTableProcessing()
    
    print(f"       >> CRIACAO DE BASES DO DUCKDB COM SUCESSO... ")
    print(f"       >> *************************************************** << ")
    logging.info(f"       >> CRIACAO DE BASES DO DUCKDB COM SUCESSO")
    #--------------------------------------------------------------------------------------------
    print(f"       >> PROCESSING DUCKDB STATUS CLINICOS... ")
    logging.info(f"       >> PROCESSING DUCKDB STATUS CLINICOS... ")
    
    processingStatusClinic()
    
    print(f"       >> FINALIZANDO PRIMEIRO PROCESSING DE DADOS... ")
    print(f"       >> *************************************************** << ")
    logging.info(f"       >> FINALIZANDO PRIMEIRO PROCESSING DE DADOS... ")
    #--------------------------------------------------------------------------------------------
    print(f"       >> PROCESSING DUCKDB GUIAS CLINICAS... ")
    logging.info(f"       >> PROCESSING DUCKDB GUIAS CLINICAS... ")
    
    processingGuias()
    
    print(f"       >> FINALIZANDO ULTIMO PROCESSING DE DADOS... ")
    print(f"       >> *************************************************** << ")
    logging.info(f"       >> FINALIZANDO ULTIMO PROCESSING DE DADOS... ")

    #--------------------------------------------------------------------------------------------
 
if __name__ == "__main__":
    print(f"   >> START PROCESSANDO DADOS DE MAMOGRAFIA014... ")
    logging.info(f"   >> START PROCESSANDO DADOS DE MAMOGRAFIA014...")
    #chama a procedure
    geraLogProcess(1)
    
    
    try:
        print(f"       >> POPULACAO MAMOGRAFIA COMP ATUAL...")
        logging.info(f"       >> POPULACAO MAMOGRAFIA COMP ATUAL...")
        popularTbleMamografiaTmp()
        print(f"       >> TERMINO POPULACAO MAMOGRAFIA COMP ATUAL...")
        logging.info(f"       >> TERMINO POPULACAO MAMOGRAFIA COMP ATUAL...")
        
        print(f"       >> INICIANDO PROCESSO PAINEL...")
        logging.info(f"       >> INICIANDO PROCESSO PAINEL...")

        main()
        logging.info(f"   >> END PROCESSANDO DADOS DE MAMOGRAFIA014...")
        
        geraLogProcess(2)
        
        print(f"       >> *************************************************** << ")
        print(f"       >> ENCERRANDO PROCESSO...")
        logging.info(f"   >> ENCERRANDO PROCESSO...")
        #sys.exit()
        # se executou com sucesso
        
    
    except Exception as e:
        logging.info(f"   >> ERRO {e} NO PROCESSO DE CARGA MV_HRP...")
        print(f"Erro: {e}")
        geraLogProcess(3)   
        #sys.exit()
        # se executou com erro  

                
                    
