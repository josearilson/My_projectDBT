/* 
   TRATAMENTO DE VENDAS (SILVER)
   O foco aqui é deixar as chaves (FKs) prontas para o JOIN com clientes e produtos, 
   além de calcular os totais e padronizar os canais de venda.
*/

-- 1. Começamos isolando oss dados brutos.  
with raw_vendas as (
    select * from {{ source('ecom_raw', 'vendas') }}
),
deduplicados as (
    select 
        *,
        row_number() over (
            partition by id_venda 
            order by data_venda desc
        ) as rn
    from raw_vendas
)

-- 2. Transformações e cálculos de negócio
select
    -- Padronizo o ID da venda seguindo a mesma regra das outras tabelas
    replace(trim(id_venda), 'sal_', 'id_') as id_venda,
    
    -- AJUSTE DE CHAVES: Aqui é crítico. Deixo os IDs de cliente e produto idênticos  
    replace(trim(id_cliente), 'cus_', 'id_') as id_cliente,
    replace(trim(id_produto), 'prd_', 'id_') as id_produto,  
    
    -- Ajusto o formato da data
    cast(data_venda as timestamp) as data_venda,
    
    -- Traduzo o canal de venda: se tiver 'Loja' vira LOJA_FISICA, se for 'Site' ou 'Ecommerce' 
    -- vira ECOMMERCE. O resto eu agrupa em 'OUTROS'.
    case 
        when upper(trim(canal_venda)) like '%LOJA%' then 'LOJA_FISICA'
        when upper(trim(canal_venda)) like '%ECOMMERCE%' or upper(trim(canal_venda)) like '%SITE%' then 'ECOMMERCE'
        else 'OUTROS'
    end as canal_venda,
    
    -- Tipagem correta: quantidade como inteiro e preço como decimal
    cast(quantidade as int) as quantidade_vendida,
    cast(preco_unitario as decimal(10,2)) as preco_unitario_valor,
    
    -- Já deixo o valor total calculado pra facilitar a vida da camada Gold
    (cast(quantidade as int) * cast(preco_unitario as decimal(10,2))) as valor_total_venda,
    
    -- Auditoria básica pra saber quando o dado foi processado
    current_timestamp() as data_processamento_silver

from deduplicados

-- 3. Filtros de qualidade
where 
    rn = 1 -- Pega só a venda mais atual
    and id_venda is not null 
    -- Bloqueio pra evitar que o cabeçalho de algum CSV perdido entre na tabela
    and id_venda != 'id_venda' 
    -- Garante que o ID siga o padrão alfanumérico esperado
    and id_venda rlike '^[a-zA-Z0-9_]+$'