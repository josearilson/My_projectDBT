/* CAMADA GOLD: DIMENSÃO DE CLIENTES
   Objetivo: Dados mestres de clientes prontos para análise de perfil e localização.
*/

{{ config(
    materialized='table',
    file_format='delta'
) }}

with silver_clientes as (
    select * from {{ ref('stg_ecom_clientes') }}
)

select
    id_cliente,
    nome_cliente,
    estado_codigo,
    pais_nome,
    data_cadastro as data_registro_original
from silver_clientes