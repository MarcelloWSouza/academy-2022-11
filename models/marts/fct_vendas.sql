with
    clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )
    ,funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )
        ,produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )
        ,pedido_item as (
        select *
        from {{ ref('int_vendas__pedido_itens') }}
    )

    ,joined_tabelas as (
        select
            pedido_item.id_pedido
            , clientes.sk_clientes as fk_cliente
            , funcionarios.sk_funcionario as fk_funcionario
            , produtos.sk_produtos as fk_produto
            , pedido_item.id_transportadora
            , pedido_item.desconto
            , pedido_item.preco_da_unidade
            , pedido_item.quantidade
            , pedido_item.data_do_pedido
            , pedido_item.frete
            , pedido_item.destinatario
            , pedido_item.endereco_destinatario
            , pedido_item.cep_destinatario
            , pedido_item.cidade_destinatario
            , pedido_item.regiao_destinatario
            , pedido_item.pais_destinatario
            , pedido_item.data_do_envio
            , pedido_item.data_requerida
            , clientes.nome_do_cliente
            , funcionarios.funcionario
            , funcionarios.gerente
            , produtos.nome_produto
            , produtos.nome_do_fornecedor
            , produtos.nome_categoria
            , produtos.is_discontinuado
        from pedido_item
        left join clientes on
        pedido_item.id_cliente = clientes.id_cliente
        left join funcionarios on
        pedido_item.id_funcionario = funcionarios.id_funcionario
        left join produtos on
        pedido_item.id_produto = produtos.id_produto
    )

    ,transformacoes as (
        select
            *
            , preco_da_unidade * quantidade as total_bruto
            , (1 - desconto) * preco_da_unidade * quantidade as total_liquido
            , case
                when desconto > 0 then true
                when desconto = 0 then false
                else false
                end as eh_desconto
        from joined_tabelas
    )

select *
from transformacoes