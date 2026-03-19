/* 
   TRATAMENTO DE PRODUTOS (SILVER)
   Objetivo aqui é deixar o catálogo de produtos limpo: padronizar IDs, 
   limpar a sujeira dos preços e organizar as categorias.
*/

-- 1. Primeiro passo: puxo os dados brutos e já resolvo o problema de duplicidade.
-- Se o mesmo produto foi editado várias vezes, eu pego só a versão mais recente.
with raw_produtos as (
    select * from {{ source('ecom_raw', 'produtos') }}
),
deduplicados as (
    select 
        *,
        row_number() over (
            partition by id_produto 
            order by data_criacao desc
        ) as rn
    from raw_produtos
)
 
select
    -- Padronizo o ID: tiro espaços e troco o prefixo 'prd_' por 'id_' pra seguir o padrão
    replace(trim(id_produto), 'prd_', 'id_') as id_produto,
    
    -- Só um trim básico no nome pra não levar espaço sobrando pro banco
    trim(nome_produto) as nome_produto,
    
    -- Deixo Categoria e Marca em MAIÚSCULAS. Isso evita que 'Eletrônicos' e 'eletronicos' 
    -- apareçam como coisas diferentes nos relatórios.
    upper(trim(categoria)) as categoria_nome,
    upper(trim(marca)) as marca_nome,
    
    -- Aqui eu limpo o campo de preço: removo cifrões ou qualquer símbolo estranho 
    -- e converto logo para Decimal pra gente conseguir fazer conta com isso.
    cast(regexp_replace(preco_atual, '[^0-9.]', '') as decimal(10,2)) as preco_valor,
    
    -- Ajusto a data para o tipo Timestamp e marco quando esse dado passou por aqui
    cast(data_criacao as timestamp) as data_cadastro,
    current_timestamp() as data_processamento_silver

from deduplicados

-- 2. Filtros finais de qualidade
where 
    rn = 1 -- Só me interessa o registro mais atualizado
    and id_produto is not null 
    -- Filtro de segurança: o ID precisa ter um formato válido (letras e números)
    and id_produto rlike '^[a-zA-Z0-9_]+$'