/* CAMADA GOLD: DIMENSÃO DE PRODUTOS
   Objetivo: Catálogo de produtos padronizado para análise de categorias e performance de vendas.
*/

{{ config(
    materialized='table',
    file_format='delta'
) }}

with silver_produtos as (
    select * from {{ ref('stg_ecom_produtos') }}
)

select
    id_produto,
    nome_produto,
    categoria_nome,
    marca_nome,
    preco_valor as preco_atual,
    data_cadastro as data_inclusao_catalogo
from silver_produtos