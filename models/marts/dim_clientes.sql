with
    clientes as (
        select *
        from {{ ref('stg_erp__clientes') }}
    )

    , transformacoes as (
        select
            row_number() over (order by id_cliente) as sk_clientes
            , id_cliente
            , nome_do_cliente
            , nome_da_empresa
            , endereco
            , cep
            , cidade
            , regiao
            , pais
        from clientes 
     )

select *
from transformacoes