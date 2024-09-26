import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './big-query-key.json'

client = bigquery.Client()

query = """
SELECT
    ano,
    CASE WHEN mes = 1 THEN 'Janeiro'
         WHEN mes = 2 THEN 'Fevereiro'
         WHEN mes = 3 THEN 'Março'
         WHEN mes = 4 THEN 'Abril'
         WHEN mes = 5 THEN 'Maio'
         WHEN mes = 6 THEN 'Junho'
         WHEN mes = 7 THEN 'Julho'
         WHEN mes = 8 THEN 'Agosto'
         WHEN mes = 9 THEN 'Setembro'
         WHEN mes = 10 THEN 'Outubro'
         WHEN mes = 11 THEN 'Novembro'
         WHEN mes = 12 THEN 'Dezembro'
         END AS mes,
    SUM(CASE WHEN CAST(b0011 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_febre,
    SUM(CASE WHEN CAST(b0011 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_febre,
    SUM(CASE WHEN CAST(b0012 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_tosse,
    SUM(CASE WHEN CAST(b0012 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_tosse,
    SUM(CASE WHEN CAST(b0013 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_dor_de_garganta,
    SUM(CASE WHEN CAST(b0013 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_dor_de_garganta,
    SUM(CASE WHEN CAST(b0014 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_falta_de_ar,
    SUM(CASE WHEN CAST(b0014 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_falta_de_ar,
    SUM(CASE WHEN CAST(b0015 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_dor_de_cabeca,
    SUM(CASE WHEN CAST(b0015 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_dor_de_cabeca,
    SUM(CASE WHEN CAST(b0016 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_dor_no_peito,
    SUM(CASE WHEN CAST(b0016 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_dor_no_peito,
    SUM(CASE WHEN CAST(b0017 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_nausea,
    SUM(CASE WHEN CAST(b0017 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_nausea,
    SUM(CASE WHEN CAST(b0018 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_nariz_entupido_escorrendo,
    SUM(CASE WHEN CAST(b0018 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_nariz_entupido_escorrendo,
    SUM(CASE WHEN CAST(b0019 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_fadiga,
    SUM(CASE WHEN CAST(b0019 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_fadiga,
    SUM(CASE WHEN CAST(b00110 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_dor_nos_olhos,
    SUM(CASE WHEN CAST(b00110 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_dor_nos_olhos,
    SUM(CASE WHEN CAST(b00111 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_perda_olfato_paladar,
    SUM(CASE WHEN CAST(b00111 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_perda_olfato_paladar,
    SUM(CASE WHEN CAST(b00112 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_dor_muscular,
    SUM(CASE WHEN CAST(b00112 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_dor_muscular,
    SUM(CASE WHEN CAST(b00113 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_diarreia,
    SUM(CASE WHEN CAST(b00113 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_diarreia,
    SUM(CASE WHEN CAST(b002 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_procurou_ajuda_medica,
    SUM(CASE WHEN CAST(b002 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_nao_procurou_ajuda_medica
FROM basedosdados.br_ibge_pnad_covid.microdados
--query sem where, pois pode influenciar nos resultados, alguem que teve tosse, pode nao saber que teve febre,
--se filtramos sempre sim ou não em todos os indicadores, podemos trazer menos resultados do que realmente existe na base
GROUP BY
    ano,
    CASE WHEN mes = 1 THEN 'Janeiro'
         WHEN mes = 2 THEN 'Fevereiro'
         WHEN mes = 3 THEN 'Março'
         WHEN mes = 4 THEN 'Abril'
         WHEN mes = 5 THEN 'Maio'
         WHEN mes = 6 THEN 'Junho'
         WHEN mes = 7 THEN 'Julho'
         WHEN mes = 8 THEN 'Agosto'
         WHEN mes = 9 THEN 'Setembro'
         WHEN mes = 10 THEN 'Outubro'
         WHEN mes = 11 THEN 'Novembro'
         WHEN mes = 12 THEN 'Dezembro'
    END;
"""

query2 = """
SELECT
    dados.ANO,
    CASE WHEN dados.mes = 1 THEN 'Janeiro'
         WHEN dados.mes = 2 THEN 'Fevereiro'
         WHEN dados.mes = 3 THEN 'Março'
         WHEN dados.mes = 4 THEN 'Abril'
         WHEN dados.mes = 5 THEN 'Maio'
         WHEN dados.mes = 6 THEN 'Junho'
         WHEN dados.mes = 7 THEN 'Julho'
         WHEN dados.mes = 8 THEN 'Agosto'
         WHEN dados.mes = 9 THEN 'Setembro'
         WHEN dados.mes = 10 THEN 'Outubro'
         WHEN dados.mes = 11 THEN 'Novembro'
         WHEN dados.mes = 12 THEN 'Dezembro'
    END AS mes,
    SUM(CASE
            WHEN CAST(dados.B009B AS INT64) = 1 OR CAST(dados.B009D AS INT64) = 1 OR CAST(dados.B009F AS INT64) = 1 THEN 1
            ELSE 0
        END) AS Testes_Positivos,
    SUM(CASE
            WHEN CAST(dados.B009B AS INT64) = 2 OR CAST(dados.B009D AS INT64) = 2 OR CAST(dados.B009F AS INT64) = 2 THEN 1
            ELSE 0
        END) AS Testes_Negativos,
    SUM(CASE
            WHEN CAST(dados.B009B AS INT64) = 3 OR CAST(dados.B009D AS INT64) = 3 OR CAST(dados.B009F AS INT64) = 3 THEN 1
            ELSE 0
        END) AS Testes_Inconclusivo,
    SUM(CASE
            WHEN CAST(dados.B009B AS INT64) = 4 OR CAST(dados.B009D AS INT64) = 4 OR CAST(dados.B009F AS INT64) = 4 THEN 1
            ELSE 0
        END) AS Testes_nao_recebeu_ainda
FROM
    `basedosdados.br_ibge_pnad_covid.microdados` dados
WHERE
    CAST(dados.B009B AS INT64) IN (1, 2, 3, 4) AND
    CAST(dados.B009D AS INT64) IN (1, 2, 3, 4) AND
    CAST(dados.B009F AS INT64) IN (1, 2, 3, 4)
GROUP BY
    dados.ANO,
    CASE WHEN dados.mes = 1 THEN 'Janeiro'
         WHEN dados.mes = 2 THEN 'Fevereiro'
         WHEN dados.mes = 3 THEN 'Março'
         WHEN dados.mes = 4 THEN 'Abril'
         WHEN dados.mes = 5 THEN 'Maio'
         WHEN dados.mes = 6 THEN 'Junho'
         WHEN dados.mes = 7 THEN 'Julho'
         WHEN dados.mes = 8 THEN 'Agosto'
         WHEN dados.mes = 9 THEN 'Setembro'
         WHEN dados.mes = 10 THEN 'Outubro'
         WHEN dados.mes = 11 THEN 'Novembro'
         WHEN dados.mes = 12 THEN 'Dezembro'
    END;
"""

query3 = """
SELECT
    ano,
    CASE WHEN mes = 1 THEN 'Janeiro'
         WHEN mes = 2 THEN 'Fevereiro'
         WHEN mes = 3 THEN 'Março'
         WHEN mes = 4 THEN 'Abril'
         WHEN mes = 5 THEN 'Maio'
         WHEN mes = 6 THEN 'Junho'
         WHEN mes = 7 THEN 'Julho'
         WHEN mes = 8 THEN 'Agosto'
         WHEN mes = 9 THEN 'Setembro'
         WHEN mes = 10 THEN 'Outubro'
         WHEN mes = 11 THEN 'Novembro'
         WHEN mes = 12 THEN 'Dezembro'
         END AS mes,
    SUM(CASE WHEN CAST(b0101 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_diagnostico_diabetes,
    SUM(CASE WHEN CAST(b0101 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_diagnostico_diabetes,
    SUM(CASE WHEN CAST(b0102 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_diagnostico_hipertensao,
    SUM(CASE WHEN CAST(b0102 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_diagnostico_hipertensao,
    SUM(CASE WHEN CAST(b0103 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_doencas_respiratorias,
    SUM(CASE WHEN CAST(b0103 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_doencas_respiratorias,
    SUM(CASE WHEN CAST(b0104 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_doencas_do_coracao,
    SUM(CASE WHEN CAST(b0104 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_doencas_do_coracao,
    SUM(CASE WHEN CAST(b0105 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_depressao,
    SUM(CASE WHEN CAST(b0105 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_depressao,
    SUM(CASE WHEN CAST(b0106 AS INT64) = 1 THEN 1 END) AS qtd_pessoas_com_cancer,
    SUM(CASE WHEN CAST(b0106 AS INT64) = 2 THEN 1 END) AS qtd_pessoas_sem_cancer
FROM basedosdados.br_ibge_pnad_covid.microdados
--query sem where, pois pode influenciar nos resultados, alguem que tem diabetes, pode nao ter hipertensao ou cancer
--se filtramos sempre sim ou não em todos os indicadores, podemos trazer menos resultados do que realmente existe na base
GROUP BY
    ano,
    CASE WHEN mes = 1 THEN 'Janeiro'
         WHEN mes = 2 THEN 'Fevereiro'
         WHEN mes = 3 THEN 'Março'
         WHEN mes = 4 THEN 'Abril'
         WHEN mes = 5 THEN 'Maio'
         WHEN mes = 6 THEN 'Junho'
         WHEN mes = 7 THEN 'Julho'
         WHEN mes = 8 THEN 'Agosto'
         WHEN mes = 9 THEN 'Setembro'
         WHEN mes = 10 THEN 'Outubro'
         WHEN mes = 11 THEN 'Novembro'
         WHEN mes = 12 THEN 'Dezembro'
    END;
"""


# query 1 => Principais Sintomas COVID

query_job = client.query(query)

# obtendo o resultado da query e salvando em um DataFrame
df_sintomas = query_job.to_dataframe()
#utilizando 3 meses para as análises
df_sintomas_3m = df_sintomas[df_sintomas['mes'].isin(['Setembro', 'Outubro', 'Novembro'])]

soma_total_por_sintoma = df_sintomas_3m.drop(columns=['mes', 'ano']).sum()

# query 2 => Testes COVID

query_job2 = client.query(query2)
df_testes = query_job2.to_dataframe()
#utilizando 3 meses para as análises
df_testes_3m = df_testes[df_testes['mes'].isin(['Setembro', 'Outubro', 'Novembro'])]


#query 3 => Comorbidades

query_job3 = client.query(query3)
df_comorbidades = query_job3.to_dataframe()
#utilizando 3 meses para as análises
df_comorbidades_3m = df_comorbidades[df_comorbidades['mes'].isin(['Setembro', 'Outubro', 'Novembro'])]

intro = """Imagine agora que você foi contratado(a) como Expert em Data Analytics por um grande hospital para entender como foi o comportamento da população na época da pandemia da COVID-19 e quais indicadores seriam importantes para o planejamento, caso haja um novo surto da doença. 
Apesar de ser contratado(a) agora, a sua área observou que a utilização do estudo do PNAD-COVID 19 do IBGE seria uma ótima base para termos boas respostas ao problema proposto, pois são dados confiáveis. Porém, não será necessário utilizar todas as perguntas realizadas na pesquisa para enxergar todas as oportunidades ali postas.

É sempre bom ressaltar que há dados triviais que precisam estar no projeto, pois auxiliam muito na análise dos dados:

Características clínicas dos sintomas;
Características da população;
Características econômicas da sociedade.
O Head de Dados pediu para que você entrasse na base de dados do PNAD-COVID-19 do IBGE e organizasse esta base para análise, utilizando Banco de Dados em Nuvem e trazendo as seguintes características:
"""

list = """a. Utilização de no máximo 20 questionamentos realizados na pesquisa;  
b. Utilizar 3 meses para construção da solução;  
c. Caracterização dos sintomas clínicos da população;    
d. Comportamento da população na época da COVID-19;  
e. Características econômicas da Sociedade;  
Seu objetivo será trazer uma breve análise dessas informações, como foi a organização do banco, as perguntas selecionadas para a resposta do problema e quais seriam as principais ações que o hospital deverá tomar em caso de um novo surto de COVID-19"""


st.title("O desafio")
st.markdown(intro)
st.markdown(list)
st.title("Principais sintomas de COVID")
st.dataframe(df_sintomas)

#grafico de sintomas

cores = ['#ff9999','#66b3ff','#99ff99','#ffcc99','#c2c2f0','#ffb3e6','#c2f0c2','#ff6666','#c299ff','#ff66b3','#f0c2f0','#66ff66','#99ccff']
st.dataframe(df_sintomas_3m)
st.dataframe(soma_total_por_sintoma)
st.bar_chart(soma_total_por_sintoma,  x_label='Número de Pessoas', y_label='Sintomas', horizontal=True)
