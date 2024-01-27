# DESAFIO TÉCNICO ENGENHARIA DE DADOS

A empresa é uma rede de farmácias com mais de 50 anos de história, e que atualmente conta com mais de 600 filiais espalhadas pelos estados do Rio Grande do Sul, Santa Catarina, Paraná e São Paulo. Além disso, a empresa investe fortemente em sua plataforma digital, oferecendo aos clientes a conveniência de comprar produtos online com a opção de entrega em domicílio ou retirada em loja. Nossa missão é proporcionar saúde e bem-estar para as pessoas, e nossa visão é ser a melhor em produtos e serviços de saúde e bem-estar de forma sustentável e inovadora! 

O time de dados da empresa recebeu o desafio de estudar e entender profundamente o comportamento de compra dos seus clientes, com objetivo de propiciar um atendimento cada vez mais personalizado, e oferecer benefícios para eles conforme estreitam seu relacionamento com a empresa. 

A primeira etapa deste desafio começa com você, nosso(a) Engenheiro(a) de Dados, com objetivo de fornecer os dados dos clientes e das suas respectivas compras, para que o time de analistas e cientistas de dados possam trabalhar. Considere que a equipe é nova, e ainda não tem ferramentas, padrões e arquitetura definida, mas tem o desejo de contar com um Datalake. Agora cabe a você, não só entregar os dados transformados para o time, mas também projetar uma arquitetura adequada para este projeto e projetos futuros. 

Ao final do desafio esperamos ver: 

- Proposta de Arquitetura: Proponha uma arquitetura, e aborde a eficiência no processamento, escalabilidade e a fluidez dos dados entre diferentes camadas do pipeline. 
- Pipeline de Dados: Desenvolva um pipeline de dados para extrair, transformar 

 e carregar os dados solicitados. 

- Garantia da Qualidade dos Dados: Apresente formas de garantir a qualidade dos dados, incluindo validações, limpezas e tratamentos necessários. 
- Apresentação: Destaque suas propostas e implementação e justifique as suas escolhas com relação a arquitetura, ferramentas e técnicas escolhidas. 

A seguir você encontra os dados mapeados pelo time de analistas de dados e um dicionário de dados com as informações sobre as origens dos dados. 


**DADOS ESPERADOS**

****Vendas****:

  - codigo\_filial
  - codigo\_cupom\_venda
  - data\_emissao
  - codigo\_item
  - valor\_unitario
  - quantidade
  - codigo\_cliente
  - tipo\_desconto (Convênio/Promoção)
  - canal\_venda (Loja/Site/App)

****Clientes****:

  - codigo\_cliente
  - data\_nascimento
  - idade 
  - sexo 
  - uf 
  - cidade 
  - estado\_civil
  - flag\_lgpd\_call
  - flag\_lgpd\_sms
  - flag\_lgpd\_email 
  - flag\_lgpd\_push


**DICIONÁRIO DE DADOS**

****Colunas da tabela __vendas__****: 

  - d\_dt\_vd: Data em que a venda foi efetuada. 
  - n\_id\_fil: Código único da filial que efetuou a venda. (PK) 
  - n\_id\_vd\_fil: Código identificador único da venda na filial. (PK) 
  - v\_cli\_cod: Código identificador do cliente da venda. 
  - n\_vlr\_tot\_vd: Valor total da venda já com descontos. 
  - n\_vlr\_tot\_desc: Valor total de descontos. 
  - v\_cpn\_eml: Se o cupom fiscal foi enviado por e-mail. 
  - tp\_pgt: Tipo de pagamento da venda. 

****Colunas da tabela __pedidos__****: 

  - n\_id\_pdd: Identificador único do pedido. (PK) 
  - d\_dt\_eft\_pdd: Data efetiva do pedido. 
  - d\_dt\_entr\_pdd: Data de entrega do pedido. 
  - v\_cnl\_orig\_pdd: Canal de origem do pedido (Loja/Site/App). 
  - v\_uf\_entr\_pdd: UF de entrega do pedido. 
  - v\_lc\_ent\_pdd: Localidade de entrega do pedido. 
  - n\_vlr\_tot\_pdd: Valor total do pedido. 

****Colunas da tabela __itens\_vendas__****: 

  - n\_id\_fil: Código único da filial que efetuou a venda. (PK) 
  - n\_id\_vd\_fil: Código identificador único da venda na filial. (PK) 
  - n\_id\_it: Código identificador do item na venda. (PK) 
  - v\_rc\_elt: Se a venda do item foi com receita eletrônica. 
  - v\_it\_vd\_conv: Se no item da venda ocorreu desconto de convênio. 
  - n\_vlr\_pis: Valor do imposto PIS. 
  - n\_vlr\_vd: Valor final do item na venda já com desconto. 
  - n\_vlr\_desc: Valor do desconto. 
  - n\_qtd: Quantidade do item. 

****Colunas da tabela __pedido\_venda__****: 

  - n\_id\_fil: Código único da filial que efetuou a venda. (PK) 
  - n\_id\_vd\_fil: Código identificador único da venda na filial. (PK) 
  - n\_id\_pdd: Número do pedido que originou a venda. (PK) 

****Colunas da tabela __clientes__****: 

  - v\_id\_cli: Código identificador do cliente. (PK) 
  - d\_dt\_nasc: Data de nascimento do cliente. 
  - v\_sx\_cli: Gênero do cliente. 
  - n\_est\_cvl: Estado civil do cliente (1 Solteiro/2 Casado/3 Viúvo/4 Desquitado /5 Divorciado/6 Outros). 

****Colunas da tabela __clientes\_opt__****: 

  - v\_id\_cli: Código identificador do cliente. (PK) 
  - b\_push: Se o cliente autoriza o recebimento de notificações via push. 
  - b\_sms: Se o cliente autoriza o recebimento de notificações via SMS. 
  - b\_email: Se o cliente autoriza o recebimento de notificações via e-mail. 
  - b\_call: Se o cliente autoriza o recebimento de notificações via ligação. 

****Colunas da tabela __enderecos\_clientes__****: 

  - v\_id\_cli: Código identificador do cliente. (PK) 
  - n\_sq\_end: Sequência de cadastro do endereço do cliente. (PK)
  - d\_dt\_exc: Data de exclusão do endereço. 
  - v\_lcl: Localidade do endereço. 
  - v\_uf: UF do endereço. 

****Complemento****: 

  - [Dados públicos de gêneros por nome:](https://data.brasil.io/dataset/genero-nomes/nomes.csv.gz)

**🧑🏽 Colaboradores**
-
Este projeto foi gerido por:

- Gustavo H Martins ([GitHub](https://github.com/Gustavo-H-Martins) | [LinkedIn](https://www.linkedin.com/in/gustavo-henrique-lopes-martins-361789192/))
