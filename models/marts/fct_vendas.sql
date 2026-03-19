/* CAMADA GOLD: FATO DE VENDAS
   Objetivo: Consolidar transações com informações de contexto (clientes/produtos/vendas).
*/

{{ config(
    materialized='table',
    file_format='delta'
) }}

with vendas as (
    select * from {{ ref('stg_ecom_vendas') }}
),

clientes as (
    select id_cliente, nome_cliente, estado_codigo from {{ ref('stg_ecom_clientes') }}
),

produtos as (
    select id_produto, nome_produto, categoria_nome, preco_valor from {{ ref('stg_ecom_produtos') }}
)

select
    v.id_venda,
    v.data_venda,
    
    -- Informações de Cliente
    v.id_cliente,
    c.nome_cliente,
    c.estado_codigo,
    
    -- Informações de Produto
    v.id_produto,
    p.nome_produto,
    p.categoria_nome,
    
    -- Métricas Financeiras
    v.canal_venda,
    v.quantidade_vendida,
    v.preco_unitario_valor,
    v.valor_total_venda,
    
    -- Auditoria
    current_timestamp() as data_carga_gold

from vendas v
left join clientes c on v.id_cliente = c.id_cliente
left join produtos p on v.id_produto = p.id_produto