with
    pedidos as (
        select *
        from {{ ref('stg_erp__ordens') }}
    )
    
    , pedidos_itens as (
        select *
        from {{ ref('stg_erp__ordem_detalhes') }}
    )

    ,joined_pedidos_itens as (
        select
            pedidos.id_pedido
            , pedidos.id_funcionario
            , pedidos.id_cliente
            , pedidos.id_transportadora
            , pedidos_itens.id_pedidos 
            , pedidos_itens.id_produto
            , pedidos_itens.desconto
            , pedidos_itens.preco_da_unidade
            , pedidos_itens.quantidade
            , pedidos.data_do_pedido
            , pedidos.frete
            , pedidos.destinatario
            , pedidos.endereco_destinatario
            , pedidos.cep_destinatario
            , pedidos.cidade_destinatario
            , pedidos.regiao_destinatario
            , pedidos.pais_destinatario
            , pedidos.data_do_envio
            , pedidos.data_requerida
        from pedidos_itens
        left join pedidos on 
            pedidos_itens.id_pedidos = pedidos.id_pedido
    ) 

select *
from joined_pedidos_itens
