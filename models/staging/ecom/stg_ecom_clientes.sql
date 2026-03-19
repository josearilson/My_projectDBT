/* 
   TRATAMENTO DE CLIENTES (SILVER)
   Aqui eu realizo a limpezaa nos dados brutos: tirando prefixos inúteis, 
   limpando títulos dos nomes e deixando as localizações no padrão.
*/

-- Primeiro, pego os dados direto da Bronze e já aproveito para marcar 
-- quem é duplicado, priorizando sempre o cadastro mais recente.
with raw_clientes as ( 
    select * from {{ source('ecom_raw', 'clientes') }}
),
deduplicados as (
    select 
        *,
        row_number() over (
            partition by id_cliente 
            order by data_cadastro desc
        ) as rn
    from raw_clientes
)
 
select
    -- Ajusto o ID: tiro espaços e mudo o prefixo 'cus_' para 'id_' pra ficar tudo padrão
    replace(trim(id_cliente), 'cus_', 'id_') as id_cliente,
    
    -- Uso um Regex para "limpar" o nome: removo Dr, Sra, etc, e deixo só o nome puro sem espaços extras
    trim(regexp_replace(nome_cliente, '(?i)\\b(Sra|Srta|Dra|Dr|Sr)\\b\\.?\\s*', '')) as nome_cliente,
     
    -- Deixo Estado e País em letras maiúsculas e sem espaços extras para evitar confusão depois
    upper(trim(estado)) as estado_codigo,
    upper(trim(pais)) as pais_nome,
    
    -- Forço a tipagem para Timestamp e registro o momento exato que o dado caiu na Silver
    cast(data_cadastro as timestamp) as data_cadastro,
    current_timestamp() as data_processamento_silver

from deduplicados

-- Filtros de segurança:
where 
    rn = 1 -- Aqui garanto que só vai passar a última versão de cada cliente
    and id_cliente is not null 
    -- Só deixo passar IDs que façam sentido (letras, números e underline)
    and id_cliente rlike '^[a-zA-Z0-9_]+$'