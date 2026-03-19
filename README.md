# 📊 Projeto dbt - Ecommerce Analytics

## 📌 Visão Geral
Este projeto utiliza o **dbt (Data Build Tool)** para transformar dados brutos de um e-commerce em modelos analíticos estruturados seguindo a arquitetura em camadas:

- **Bronze (Raw)** → Dados brutos no Databricks
- **Silver (Staging)** → Limpeza e padronização
- **Gold (Marts)** → Modelos analíticos (dimensões e fatos)

---

## 🧱 Estrutura do Projeto

```
models/
│
├── staging/
│   └── ecom/
│       ├── stg_ecom_clientes.sql
│       ├── stg_ecom_produtos.sql
│       ├── stg_ecom_vendas.sql
│       └── stg_ecom.yml
│
├── marts/
│   ├── dim_clientes.sql
│   ├── dim_produtos.sql
│   ├── dim_vendas.sql
│   ├── fct_vendas.sql
│   └── marts.yml
│
└── sources.yml
```

---

## ⚙️ Configuração do Projeto

Arquivo `dbt_project.yml`:

- **Nome:** ecommerce_analytics
- **Staging:** materializado como `view` (schema: silver)
- **Marts:** materializado como `table` (schema: gold)
- Uso de recursos modernos:
  - Iceberg Tables
  - Microbatch
  - Materialization V2

---

## 🔗 Fonte de Dados

Definida em `sources.yml`:

- Catálogo: `workspace`
- Schema: `ecommerce`
- Tabelas:
  - clientes
  - produtos
  - vendas

---

## 🥈 Camada Silver (Staging)

Responsável por:

- Limpeza de dados
- Padronização de campos
- Remoção de duplicidades
- Tipagem correta

### Exemplos de Tratamento

**Clientes**
- Remoção de prefixos (ex: `cus_` → `id_`)
- Limpeza de nomes (remoção de títulos como Dr., Sra.)
- Padronização de país/estado
- Deduplicação por `row_number`

**Produtos**
- Conversão de preços
- Padronização de IDs

**Vendas**
- Padronização de chaves
- Cálculo de métricas (valor total)

---

## 🥇 Camada Gold (Marts)

Modelos finais para consumo analítico:

### Dimensões
- `dim_clientes`
- `dim_produtos`
- `dim_vendas`

### Fato
- `fct_vendas`

Responsável por:
- Modelagem dimensional
- Otimização para BI (Power BI, etc.)
- Métricas consolidadas

---

## ✅ Testes de Qualidade

Definidos nos arquivos `.yml`:

- `not_null`
- `unique`

Garantem:
- Integridade de chaves
- Qualidade dos dados

---

## 🚀 Como Executar

```bash
dbt run
dbt test
```

---

## 📊 Benefícios da Arquitetura

- Separação clara entre camadas
- Facilidade de manutenção
- Escalabilidade
- Pronto para BI e Analytics

---

## 🧠 Boas Práticas Aplicadas

- Uso de `source()` para rastreabilidade
- CTEs para organização
- Padronização de nomenclatura (`stg_`, `dim_`, `fct_`)
- Testes automatizados
- Documentação via YAML

---

## 📎 Observações

Este projeto segue boas práticas de Engenharia de Dados modernas utilizando:

- Databricks
- Unity Catalog
- dbt

---
 
