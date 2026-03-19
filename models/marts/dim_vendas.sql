{{ config(
    materialized='table',
    file_format='delta'
) }}

/* 
   Aqui chamamos a limpeza feita na staging.  
*/

select * from {{ ref('stg_ecom_vendas') }}