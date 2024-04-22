# Desafio de Análise de Dados da Transfeera

Olá,

Se você recebeu este desafio significa que você avançou para a próxima etapa do nosso processo seletivo 🎉 e estamos ansiosos para ver suas habilidades em ação.

Abaixo está o desafio que preparamos para você. Sinta-se à vontade para usar a linguagem de programação ou ferramentas analíticas de sua preferência. Não há necessidade de se preocupar com formalidades, queremos que você se sinta confortável para resolver o problema da maneira que considerar mais eficaz.

## Contexto

Você faz parte da equipe de dados da Transfeera e a gente atende todas as áreas da empresa. Sendo assim, o time de Customer Success (CS) nos procurou, pois precisa entender melhor o comportamento dos clientes para tomar algumas decisões diárias.

### Dores de CS

1. **Análise do Volume de Transferências por Cliente**: O gestor de CS precisa entender o volume de transferências realizado por cada cliente ao longo do tempo para identificar padrões de comportamento e possíveis oportunidades de engajamento ou retenção.

2. **Identificação dos Dias de Utilização do Produto**: É importante determinar os dias em que cada cliente utiliza ativamente nosso produto, bem como os dias em que não o utiliza. Isso pode fornecer insights valiosos sobre o engajamento do cliente e ajudar a orientar estratégias de comunicação e suporte.

3. **Previsão do Faturamento Diário dos Clientes**: Atualmente, não temos uma visão em tempo real do faturamento que cada cliente está gerando. O gestor de CS gostaria de acompanhar diariamente o valor que cada cliente está prestes a pagar à nossa empresa, permitindo uma melhor gestão financeira e uma compreensão mais clara do desempenho do cliente ao longo do mês.

4. **Além das dores… Exploração de Novas Análises**: O gestor de CS está aberto a sugestões e análises adicionais que possam enriquecer o projeto e fornecer insights valiosos para a equipe.

### Estrutura de Dados

O produto da Transfeera permite que cada cliente tenha uma conta conosco e realize várias transações diariamente, entre elas 'payout', 'payin' ou 'boleto', com os valores entrando e saindo da sua respectiva conta.

De forma simples: 
- **payouts**: pagamentos realizados via PIX, cujo dinheiro saiu da conta do cliente e foi para a conta de um terceiro;
- **boletos**: pagamentos de boleto, cujo dinheiro saiu da conta do cliente e foi para a conta de um terceiro;
- **payins**: recebimentos, onde o dinheiro vem da conta de um terceiro e é creditada na conta do cliente.

Para construir uma primeira versão dessa entrega, as tabelas estão disponibilizadas por arquivos csv. A ideia é que elas sejam alimentadas no ambiente analítico no Google Cloud uma vez ao dia, vindo de um banco PostgreSQL que está na AWS.

### Quais tabelas temos acesso?

1. **Transações**: Armazena informações sobre as transferências realizadas pelos clientes, que podem ser 'payout', 'payin' ou 'boleto'.
2. **Contratos**: Contém informações contratuais, como vigência e taxas para cada tipo de transação.
3. **Clientes**: Armazena informações básicas dos clientes.

## Tarefas

1. Desenvolva um ou mais modelos de dados que abordem as dores do gestor de CS de maneira abrangente, preparando os dados para conexão com uma ferramenta de visualização de dados.
2. As tabelas disponibilizadas são os dados brutos, vindas direto no nosso banco de produção. Caso veja necessidade, sinta-se livre para criar camadas de modelagem.
3. Sugira uma estratégia para garantir que esses modelos estejam sempre atualizados com os dados mais recentes, considerando a necessidade de atualização diária.
4. Apresentar as análises possíveis a partir dos modelos que você criou ganha pontos a mais, afinal, vamos precisar auxiliar o time de CS a montar as visões para sanar as dores citadas antes.

## Instruções de entrega

Envie suas respostas até a data de conclusão do teste.

Não tem melhor forma e nem ferramenta, use aquilo que fizer mais sentido pra você. Precisamos apenas que você envie seu código, scripts ou notebooks juntamente com um documento explicando suas análises, interpretações, insights e o que mais achar necessário.

Se tiver alguma dúvida, entre em contato conosco.

Boa sorte e aguardamos ansiosamente para ver suas soluções 🤩
